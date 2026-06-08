import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter

import subprocess

def run_program(language, filename, input_data):
    language = language.lower()

    try:
        if language == "python":
            try:
                run_res = subprocess.run(
                    ["python", filename],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "cpp" or language == "c++":
            compile_res = subprocess.run(
                ["g++", filename, "-o", "a.exe"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    ["a.exe"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "rust":
            compile_res = subprocess.run(
                ["rustc", filename, "-o", "main.exe"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    ["main.exe"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "kotlin":
            kotlinc_path = r"E:\kotlinc\bin\kotlinc.BAT"
            java_path = r"C:\Program Files (x86)\Common Files\Oracle\Java\java8path\java.EXE"

            compile_res = subprocess.run(
                [kotlinc_path, filename, "-include-runtime", "-d", "main.jar"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    [java_path, "-jar", "main.jar"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=3
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        else:
            return "", f"Unsupported language: {language}"

    except Exception as e:
        return "", str(e)
def get_filename(language):
    if language == "python":
        return "test.py"
    elif language == "cpp":
        return "test.cpp"
    elif language=="C++":
        return "test.cpp"
    elif language == "java":
        return "Main.java"
    elif language == "js":
        return "test.js"
    elif language == "go":
        return "intersperse_test.go"
    elif language == "rust" or language=="Rust":
        return "testbox/rust/src/main.rs"
    elif language == "kotlin":
        return "testbox/kotlin/main.kt"
    else:
        raise ValueError(f"Unsupported language: {language}")
def write2file(filename, content):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
def file2text(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()
    return text
def clean_check_file(dataset, language, input_file="data.txt"):
    if language not in dataset.data:
        dataset.load(language)
    data_uid_set = set(
        item.get("src_uid")
        for item in dataset.data[language]
        if item.get("src_uid")
    )
    with open(input_file, "r", encoding="utf-8") as f:
        file_uids = [line.strip() for line in f if line.strip()]
    filtered_uids = [uid for uid in file_uids if uid in data_uid_set]

    with open(input_file, "w", encoding="utf-8") as f:
        for uid in filtered_uids:
            f.write(uid + "\n")

    print(f"原始数量: {len(file_uids)}")
    print(f"保留数量: {len(filtered_uids)}")
    print(f"删除数量: {len(file_uids) - len(filtered_uids)}")
import json

def extract_by_src_uid(dataset, language, input_file="data.txt", output_file="C#.jsonl"):
    # 读取 uid
    with open(input_file, "r", encoding="utf-8") as f:
        uid_set = set(line.strip() for line in f if line.strip())

    # 加载数据
    if language not in dataset.data:
        dataset.load(language)

    count = 0
    with open(output_file, "w", encoding="utf-8") as out:
        for item in dataset.data[language]:
            src_uid = item.get("src_uid")

            if src_uid not in uid_set:
                continue

            # ✅ 只保留主数据
            record = {
                "src_uid": src_uid,
                "bug_code_uid": item.get("bug_code_uid"),
                "apr_id": item.get("apr_id"),
                "lang": item.get("lang"),
                "lang_cluster": item.get("lang_cluster"),
                "difficulty": item.get("difficulty"),
                "tags": item.get("tags"),
                "bug_exec_outcome": item.get("bug_exec_outcome"),
                "potential_dominant_fix_op": item.get("potential_dominant_fix_op"),
                "similarity_score": item.get("similarity_score"),
                "bug_source_code": item.get("bug_source_code")
            }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    print(f"写入完成，共 {count} 条 → {output_file}")
def count_outcome(dataset, language):
    outcomes = []

    for item in dataset.data[language]:
        outcomes.append(item.get("bug_exec_outcome"))

    counter = Counter(outcomes)
    return counter
def clean_code(text: str) -> str:
    """
    提取 markdown 中的 python 代码块。
    如果没有代码块则返回原文本。
    """

    pattern = r"```(?:python)?\s*\n(.*?)```"

    matches = re.findall(
        pattern,
        text,
        flags=re.DOTALL | re.IGNORECASE
    )

    if matches:
        return "\n\n".join(
            code.strip()
            for code in matches
        )

    return text.strip()


def _inject_kwargs(kwargs):
    kw = dict(kwargs)
    label = kw.pop("exec_label", "") or ""
    stdin = kw.pop("stdin", kw.pop("input", ""))
    if stdin is None:
        stdin = ""
    return {
        "mode": kw.get("inject_mode", "half"),
        "value": kw.get("inject_value", 10),
        "timeout": kw.get("timeout", 10),
        "max_rounds": kw.get("inject_max_rounds", 32),
        "enabled": kw.get("inject_backoff", True),
        "label": label,
        "stdin": stdin,
    }, kw


def run_code(code: str, input_str: str = "", timeout=10, **kwargs):
    from injector import Injector

    inj, _rest = _inject_kwargs({
        "timeout": timeout,
        "input": input_str,
        "stdin": input_str,
        **kwargs,
    })
    stdout, code_exit = Injector.run_with_backoff(code, **inj)
    if code_exit == 255:
        return stdout, "timeout"
    if code_exit != 0:
        return stdout, f"exit_{code_exit}"
    return stdout, ""


def run_solve(code: str, input_str: str, timeout=10, **kwargs):
    """直接执行生成的脚本，经 stdin 喂入测例（main 里 sys.stdin.read()）。"""    
    code = clean_code(code)
    if not code.strip():
        return "", "empty code"
    return run_code(code, input_str=input_str, timeout=timeout, **kwargs)


def run_solve_plain(code: str, input_str: str, timeout: int = 10) -> tuple:
    """纯子进程执行：stdin 喂入测例，无注入退避、无重试。"""
    code = clean_code(code)
    if not code.strip():
        return "", "empty code"

    path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".py",
            delete=False,
            encoding="utf-8",
        ) as f:
            f.write(code)
            path = f.name

        run_res = subprocess.run(
            [sys.executable, path],
            input=input_str,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
        stderr = (run_res.stderr or "").strip()
        stdout = (run_res.stdout or "").strip()
        if run_res.returncode != 0:
            return stdout, stderr or f"exit_{run_res.returncode}"
        return stdout, ""
    except subprocess.TimeoutExpired:
        return "", "timeout"
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def run_solve_ok(stderr: str) -> bool:
    """子进程是否正常结束（非超时、非 exit 报错、非空代码）。"""
    return (stderr or "") == ""


def normalize_output(text: str) -> str:
    return (text or "").replace("\r\n", "\n").strip()


def outputs_match(actual: str, expected: str) -> bool:
    return normalize_output(actual) == normalize_output(expected)


def solver_passes_all_cases(
    code: str,
    inputs: list,
    expected_outputs: list,
    **run_kw,
) -> bool:
    """单份代码是否通过全部 (input, output) 测例。"""
    code = clean_code(code)
    if not code.strip():
        return False
    if len(inputs) != len(expected_outputs) or not inputs:
        return False
    for inp, exp in zip(inputs, expected_outputs):
        stdout, stderr = run_solve(code, inp, **run_kw)
        if not run_solve_ok(stderr) or not outputs_match(stdout, exp):
            return False
    return True


def solver_pass_at_1(
    solver_codes: list,
    inputs: list,
    expected_outputs: list,
    **run_kw,
) -> bool:
    """pass@1：任一 solver 候选通过全部测例即为 True。"""
    for code in solver_codes:
        if solver_passes_all_cases(code, inputs, expected_outputs, **run_kw):
            return True
    return False


def _run_solve_worker(payload):
    """ProcessPool 可 pickle 的顶层函数。返回 (stdout, stderr)。"""
    code, input_str, kwargs = payload
    stdout, stderr = run_solve(code, input_str, **kwargs)
    return stdout.strip(), stderr or ""