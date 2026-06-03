from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# xcodeeval lang_cluster -> 内部运行器 key
_LANG_ALIASES = {
    "c": "c",
    "c++": "cpp",
    "cpp": "cpp",
    "python": "python",
    "java": "java",
    "javascript": "javascript",
    "js": "javascript",
    "kotlin": "kotlin",
    "rust": "rust",
    "go": "go",
    "php": "php",
    "ruby": "ruby",
    "c#": "csharp",
    "csharp": "csharp",
}

# 单条 unittest 运行超时（秒）：快语言短、JVM/Mono/Kotlin 等偏长
_LANG_RUN_TIMEOUT: Dict[str, int] = {
    "c": 2,
    "cpp": 2,
    "python": 2,
    "go": 2,
    "rust": 3,
    "javascript": 3,
    "php": 2,
    "ruby": 2,
    "java": 10,
    "kotlin": 15,
    "csharp": 12,
}

# 编译超时（秒）
_LANG_COMPILE_TIMEOUT: Dict[str, int] = {
    "c": 15,
    "cpp": 20,
    "python": 5,
    "go": 25,
    "rust": 25,
    "javascript": 5,
    "php": 5,
    "ruby": 5,
    "java": 30,
    "kotlin": 90,
    "csharp": 30,
}


def resolve_timeouts_for_lang(
    lang: str,
    *,
    run_timeout: Optional[int] = None,
    compile_timeout: Optional[int] = None,
) -> Tuple[int, int]:
    """
    解析某语言的运行/编译超时。
    run_timeout / compile_timeout 为 None 或 <=0 时，使用该语言默认值。
  """
    key = _normalize_language(lang)
    rt = int(run_timeout) if run_timeout and run_timeout > 0 else _LANG_RUN_TIMEOUT.get(key, 5)
    ct = int(compile_timeout) if compile_timeout and compile_timeout > 0 else _LANG_COMPILE_TIMEOUT.get(key, 30)
    return rt, ct


@dataclass
class RunResult:
    """单条样本（一份 code + 一组 testcases）的评测结果。"""

    ok: bool
    status: str  # pass | compile_error | runtime_error | wrong_answer | timeout | unsupported_language | no_testcases | empty_code
    message: str = ""
    passed: int = 0
    total: int = 0
    fail_case_index: Optional[int] = None
    compile_stderr: str = ""
    run_stdout: str = ""
    run_stderr: str = ""
    expected: str = ""
    actual: str = ""
    cases: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _normalize_language(language: str) -> str:
    key = (language or "").strip().lower()
    return _LANG_ALIASES.get(key, key)


def _strip_code_fence(code: str) -> str:
    text = (code or "").strip()
    if not text.startswith("```"):
        return text
    m = re.match(r"^```[\w+#.-]*\s*\n?", text)
    if m:
        text = text[m.end() :]
    if text.endswith("```"):
        text = text[: text.rfind("```")].rstrip()
    return text


def _java_rename_first_class_to_main(source: str) -> str:
    """
    Java 评测需要 `Main.java` 与主类名匹配，否则 `javac Main.java` 会报错：
    - public class Foo -> 文件名必须 Foo.java

    这里按你的需求：把源码里“第一个类名”统一替换成 Main（优先 public class）。
    """
    s = source or ""

    # 1) 优先替换第一个 public class XXX
    pat_public = re.compile(r"\bpublic\s+(?:final\s+)?class\s+([A-Za-z_]\w*)\b", re.MULTILINE)
    m = pat_public.search(s)
    if m:
        old = m.group(1)
        if old != "Main":
            s = s[: m.start(1)] + "Main" + s[m.end(1) :]
        return s

    # 2) 没有 public class 时，替换第一个 class XXX（尽量不碰 inner class：仍可能误中，但已满足“第一个类名”）
    pat_class = re.compile(r"\b(?:final\s+)?class\s+([A-Za-z_]\w*)\b", re.MULTILINE)
    m2 = pat_class.search(s)
    if m2:
        old = m2.group(1)
        if old != "Main":
            s = s[: m2.start(1)] + "Main" + s[m2.end(1) :]
    return s


def _kotlin_strip_package_line(source: str) -> str:
    """
    xcodeeval 的 Kotlin 结果里经常带 `package xxx`，但我们这里用单文件 `main.kt` 编译运行，
    为了避免包名导致的入口/文件布局问题，按你的要求自动删除 package 行（仅删除第一条 package 声明）。
    """
    s = source or ""
    # 只删除第一个 package ... 行（允许前面有空白/注释/换行）
    return re.sub(r"(?m)^\s*package\s+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\s*\r?\n", "", s, count=1)


def _expected_output(case: Dict[str, Any]) -> str:
    out = case.get("output")
    if isinstance(out, list) and out:
        return str(out[0]).strip()
    if isinstance(out, str):
        return out.strip()
    return ""


def _case_input(case: Dict[str, Any]) -> str:
    raw = case.get("input", "")
    return raw if isinstance(raw, str) else str(raw)


def _lang_filename(lang: str) -> str:
    mapping = {
        "c": "main.c",
        "cpp": "main.cpp",
        "python": "model.py",
        "java": "Main.java",
        "javascript": "main.js",
        "kotlin": "main.kt",
        "rust": "main.rs",
        "go": "main.go",
        "php": "main.php",
        "ruby": "main.rb",
        "csharp": "main.cs",
    }
    if lang not in mapping:
        raise ValueError(f"unsupported language: {lang!r}")
    return mapping[lang]


def _find_cmd(candidates: List[str]) -> Optional[str]:
    for name in candidates:
        if shutil.which(name):
            return name
    return None


def _compile(
    lang: str,
    workdir: str,
    src_path: str,
    *,
    timeout: int,
) -> Tuple[bool, str, Optional[str]]:
    """
    返回 (ok, stderr, executable_path)。
    解释型语言 executable_path 为 None，运行时用解释器 + src_path。
    """
    name = os.path.basename(src_path)

    if lang == "python":
        return True, "", None

    if lang == "c":
        gcc = _find_cmd(["gcc"])
        if not gcc:
            return False, "gcc not found", None
        out = os.path.join(workdir, "main")
        res = subprocess.run(
            [gcc, name, "-O2", "-o", out],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", out

    if lang == "cpp":
        gxx = _find_cmd(["g++"])
        if not gxx:
            return False, "g++ not found", None
        out = os.path.join(workdir, "main")
        res = subprocess.run(
            [gxx, name, "-O2", "-std=c++17", "-o", out],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", out

    if lang == "rust":
        rustc = _find_cmd(["rustc"])
        if not rustc:
            return False, "rustc not found", None
        out = os.path.join(workdir, "main")
        res = subprocess.run(
            [rustc, name, "-O", "-o", out],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", out

    if lang == "java":
        javac = _find_cmd(["javac"])
        if not javac:
            return False, "javac not found", None
        res = subprocess.run(
            [javac, name],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", workdir

    if lang == "kotlin":
        kotlinc = _find_cmd(["kotlinc"])
        if not kotlinc:
            return False, "kotlinc not found", None
        jar = os.path.join(workdir, "main.jar")
        res = subprocess.run(
            [kotlinc, name, "-include-runtime", "-d", jar],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", jar

    if lang == "csharp":
        mcs = _find_cmd(["mcs", "dmcs"])
        if not mcs:
            return False, "mcs not found (install mono-devel)", None
        # 仅单文件编译：main.cs -> main.exe
        out = os.path.join(workdir, "main.exe")
        res = subprocess.run(
            [mcs, name, "-optimize+", f"-out:{out}"],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", out

    if lang == "go":
        go = _find_cmd(["go"])
        if not go:
            return False, "go not found", None
        out = os.path.join(workdir, "main")
        # 只编译一次，后续每个 testcase 直接运行二进制
        res = subprocess.run(
            [go, "build", "-o", out, name],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if res.returncode != 0:
            return False, res.stderr or res.stdout or "compile failed", None
        return True, "", out

    if lang in {"javascript", "php", "ruby"}:
        return True, "", None

    return False, f"unsupported language: {lang}", None


def _run_one(
    lang: str,
    workdir: str,
    src_path: str,
    executable: Optional[str],
    input_data: str,
    *,
    run_timeout: int,
) -> Tuple[str, str, Optional[str]]:
    """
    执行单条用例。返回 (stdout, stderr, error_kind)。
    error_kind: None | timeout
    """
    name = os.path.basename(src_path)

    try:
        if lang == "python":
            py = _find_cmd(["python3", "python"]) or "python3"
            proc = [py, name]
            cwd = workdir
        elif lang == "java":
            java = _find_cmd(["java"]) or "java"
            proc = [java, "-cp", workdir, "Main"]
            cwd = workdir
        elif lang == "kotlin":
            java = _find_cmd(["java"]) or "java"
            proc = [java, "-jar", executable or ""]
            cwd = workdir
        elif lang == "javascript":
            node = _find_cmd(["node"]) or "node"
            proc = [node, name]
            cwd = workdir
        elif lang == "go":
            proc = [executable or ""]
            cwd = workdir
        elif lang == "php":
            php = _find_cmd(["php"]) or "php"
            proc = [php, name]
            cwd = workdir
        elif lang == "ruby":
            ruby = _find_cmd(["ruby"]) or "ruby"
            proc = [ruby, name]
            cwd = workdir
        elif lang == "csharp":
            mono = _find_cmd(["mono"])
            if not mono:
                return "", "mono not found (install mono-runtime)", "runtime_error"
            proc = [mono, executable or ""]
            cwd = workdir
        else:
            proc = [executable or ""]
            cwd = workdir

        res = subprocess.run(
            proc,
            cwd=cwd,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=run_timeout,
        )
        return res.stdout, res.stderr, None
    except subprocess.TimeoutExpired:
        return "", "TIME_LIMIT_EXCEEDED", "timeout"
    except Exception as e:
        return "", str(e), "runtime_error"


def _outputs_equal(actual: str, expected: str) -> bool:
    return actual.strip() == expected.strip()


def run(
    language: str,
    code: str,
    testcases: List[Dict[str, Any]],
    *,
    compile_timeout: Optional[int] = None,
    run_timeout: Optional[int] = None,
) -> Dict[str, Any]:
    """
  编译并运行 code，按 testcases 逐条比对 output[0]。

  Parameters
  ----------
  language : str
      xcodeeval 语言名，如 C / C++ / Python / Kotlin
  code : str
      待测完整源码（可含 markdown 代码块，会自动剥离）
  testcases : list
      unittest_db[src_uid] 格式：[{"input": "...", "output": ["..."]}, ...]

  Returns
  -------
  dict
      RunResult.to_dict()，主要字段：
      - ok: 是否全部通过
      - status: pass | compile_error | runtime_error | wrong_answer | timeout | ...
      - passed / total
      - fail_case_index: 首个失败用例下标（0-based）
      - compile_stderr / run_stdout / run_stderr / expected / actual
      - cases: 每条用例的简要结果列表
    """
    lang = _normalize_language(language)
    source = _strip_code_fence(code)
    if lang == "java":
        source = _java_rename_first_class_to_main(source)
    if lang == "kotlin":
        source = _kotlin_strip_package_line(source)

    if not testcases:
        r = RunResult(ok=False, status="no_testcases", message="empty testcases", total=0)
        return r.to_dict()

    if not source.strip():
        r = RunResult(ok=False, status="empty_code", message="empty code", total=len(testcases))
        return r.to_dict()

    if lang not in _LANG_ALIASES.values():
        r = RunResult(
            ok=False,
            status="unsupported_language",
            message=f"unsupported language: {language!r}",
            total=len(testcases),
        )
        return r.to_dict()

    try:
        filename = _lang_filename(lang)
    except ValueError as e:
        r = RunResult(ok=False, status="unsupported_language", message=str(e), total=len(testcases))
        return r.to_dict()

    run_timeout_s, compile_timeout_s = resolve_timeouts_for_lang(
        lang, run_timeout=run_timeout, compile_timeout=compile_timeout
    )

    case_rows: List[Dict[str, Any]] = []
    passed = 0
    total = len(testcases)

    with tempfile.TemporaryDirectory(prefix="xcodeeval_run_") as workdir:
        src_path = os.path.join(workdir, filename)
        with open(src_path, "w", encoding="utf-8") as f:
            f.write(source)

        ok_compile, compile_stderr, executable = _compile(
            lang, workdir, src_path, timeout=compile_timeout_s
        )
        if not ok_compile:
            r = RunResult(
                ok=False,
                status="compile_error",
                message="compilation failed",
                passed=0,
                total=total,
                compile_stderr=(compile_stderr or "")[:2000],
                cases=case_rows,
            )
            return r.to_dict()

        for idx, case in enumerate(testcases):
            inp = _case_input(case)
            expected = _expected_output(case)
            stdout, stderr, err_kind = _run_one(
                lang,
                workdir,
                src_path,
                executable,
                inp,
                run_timeout=run_timeout_s,
            )
            actual = (stdout or "").strip()
            stderr = (stderr or "").strip()

            row = {
                "index": idx,
                "status": "pass",
                "expected": expected,
                "actual": actual,
                "stderr": stderr[:500],
            }

            if err_kind == "timeout" or stderr == "TIME_LIMIT_EXCEEDED":
                row["status"] = "timeout"
                case_rows.append(row)
                r = RunResult(
                    ok=False,
                    status="timeout",
                    message=f"timeout at case {idx}",
                    passed=passed,
                    total=total,
                    fail_case_index=idx,
                    run_stdout=actual[:2000],
                    run_stderr=stderr[:2000],
                    expected=expected,
                    actual=actual,
                    cases=case_rows,
                )
                return r.to_dict()

            if stderr and stderr != "TIME_LIMIT_EXCEEDED":
                row["status"] = "runtime_error"
                case_rows.append(row)
                r = RunResult(
                    ok=False,
                    status="runtime_error",
                    message=f"runtime error at case {idx}",
                    passed=passed,
                    total=total,
                    fail_case_index=idx,
                    run_stdout=actual[:2000],
                    run_stderr=stderr[:2000],
                    expected=expected,
                    actual=actual,
                    cases=case_rows,
                )
                return r.to_dict()

            if not _outputs_equal(actual, expected):
                row["status"] = "wrong_answer"
                case_rows.append(row)
                r = RunResult(
                    ok=False,
                    status="wrong_answer",
                    message=f"wrong answer at case {idx}",
                    passed=passed,
                    total=total,
                    fail_case_index=idx,
                    run_stdout=actual[:2000],
                    run_stderr=stderr[:2000],
                    expected=expected,
                    actual=actual,
                    cases=case_rows,
                )
                return r.to_dict()

            passed += 1
            case_rows.append(row)

    r = RunResult(
        ok=True,
        status="pass",
        message="all testcases passed",
        passed=passed,
        total=total,
        cases=case_rows,
    )
    return r.to_dict()
