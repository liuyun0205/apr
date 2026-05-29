from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

# 允许从子目录直接运行：把仓库根目录加入 sys.path
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from LLM import DEFAULT_API_BASE_URL, LLM, LLMConfig, resolve_api_base_url  # noqa: E402
from runner import run as run_testcases  # noqa: E402
from utils import file2text  # noqa: E402

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None


def parseArgs() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="xcodeeval：LLM 生成修复代码 / 对结果目录跑 unittest 评测")
    p.add_argument(
        "--score-dir",
        type=str,
        default="",
        help="结果目录（如 results/direct_GPT_5.4）；指定后只评测该目录下各语言/*.txt，不调 LLM",
    )
    p.add_argument(
        "--langs",
        type=str,
        default="",
        help="仅处理指定语言（逗号分隔）。score 模式按 score-dir/<语言>/ 过滤；生成模式通常不需要。",
    )
    p.add_argument(
        "--data-root",
        type=str,
        default="datasets/xcodeeval/sub_test",
        help="score 模式：jsonl 元数据目录（按 <语言>.jsonl 对齐 bug_code_uid）",
    )
    p.add_argument(
        "--score-report",
        type=str,
        default="",
        help="score 模式：评测报告输出路径（默认 <score-dir>/eval_report.json）",
    )
    p.add_argument("--data", type=str, default="", help="样本 jsonl 路径（生成模式必填）")
    p.add_argument(
        "--results-root",
        type=str,
        default="results",
        help="结果根目录（默认 results；实际路径为 <root>/<范式>_<模型>/<语言>/）",
    )
    p.add_argument(
        "--paradigm",
        type=str,
        default="",
        help="范式名称（用于目录名；默认与 --model-type 相同，如 api / local）",
    )
    p.add_argument(
        "--result-dir",
        type=str,
        default="",
        help="显式指定运行目录（含范式与模型层，不含语言子目录）；为空则自动生成",
    )
    p.add_argument(
        "--descriptions",
        type=str,
        default="datasets/xcodeeval/problem_descriptions.jsonl",
        help="problem_descriptions.jsonl 路径",
    )
    p.add_argument(
        "--tests",
        type=str,
        default="datasets/xcodeeval/unittest_db.json",
        help="unittest_db.json（score 模式用于拉取用例）",
    )
    p.add_argument(
        "--run-timeout",
        type=int,
        default=0,
        help="score：单条 unittest 运行超时（秒）；0=按语言自动（C/Python 2s，Java 10s，Kotlin 15s 等）",
    )
    p.add_argument(
        "--compile-timeout",
        type=int,
        default=0,
        help="score：编译超时（秒）；0=按语言自动（Kotlin 90s，C++ 20s 等）",
    )

    p.add_argument("--id", type=str, default=None, help="只评测指定 id（默认字段 src_uid）")
    p.add_argument(
        "--index",
        type=int,
        default=None,
        help="只评测指定 0 起始行号（通过 sub_test_index.json 解析为 --id；与 --id 二选一）",
    )
    p.add_argument("--id-field", type=str, default="src_uid", help="id 字段名（默认 src_uid）")
    p.add_argument(
        "--out-id-field",
        type=str,
        default="index",
        help="结果文件名：index=0 起始行号；或 bug_code_uid / src_uid 等字段名",
    )
    p.add_argument("--max-items", type=int, default=0, help="最多评测多少条（0 表示不限）")
    p.add_argument("--print-every", type=int, default=10, help="每处理 N 条打印一次进度（含调用 LLM 前后）")
    p.add_argument("--resume", action="store_true", help="断电续传：跳过 result 中已存在的 <id>.txt")
    p.add_argument(
        "--lang-summary-dir",
        type=str,
        default="",
        help="扫描该目录下所有 *.jsonl 汇总各语言完成情况（默认与 --data 同目录）",
    )
    p.add_argument(
        "--no-lang-summary",
        action="store_true",
        help="不打印各语言已完成/未完成汇总",
    )

    # LLM
    p.add_argument(
        "--model-type",
        type=str,
        default="api",
        choices=["api", "local"],
        help="api=远程 API；local=本地 Transformers",
    )
    p.add_argument("--model", type=str, default="", help="模型名（生成模式必填）")
    p.add_argument(
        "--base-url",
        type=str,
        default=DEFAULT_API_BASE_URL,
        help=f"直连 API 地址（默认 {DEFAULT_API_BASE_URL}）",
    )
    p.add_argument(
        "--prompt",
        type=str,
        default="",
        help="system prompt 文件（默认仓库根目录 prompt.txt）",
    )
    p.add_argument("--llm-timeout", type=int, default=240, help="单条样本调用 LLM 的超时（秒）")
    p.add_argument("--heartbeat", type=int, default=30, help="LLM 调用期间每隔 N 秒打印一次心跳（0 关闭）")
    p.add_argument("--retry", type=int, default=0, help="LLM 失败时重试次数（0 表示无限重试）")
    p.add_argument(
        "--retry-sleep",
        type=float,
        default=5.0,
        help="每条样本 LLM 失败后重试前睡眠秒数（503 等建议 >=5）",
    )
    p.add_argument("--no-tqdm", action="store_true", help="禁用 tqdm（若未安装 tqdm 也会自动禁用）")

    # 输出（保留参数，兼容旧命令）
    p.add_argument("--outdir", type=str, default="eval_outputs", help="保留参数（不再使用）")
    p.add_argument("--save-fail", action="store_true", help="保留参数（不再使用）")
    return p.parse_args()


def resolveIndexArg(args: argparse.Namespace) -> int:
    """若指定 --index，通过 sub_test 映射解析为 --id。成功返回 0。"""
    if args.index is None:
        return 0
    if args.id:
        print("--index 与 --id 不能同时指定", file=sys.stderr)
        return 2
    data_path = (args.data or "").strip()
    if not data_path:
        print("使用 --index 时需要 --data 以推断语言", file=sys.stderr)
        return 2
    try:
        from sub_test_map import get_map, lang_from_path
    except ImportError:
        from datasets.xcodeeval.sub_test_map import get_map, lang_from_path

    lang = lang_from_path(data_path)
    args.id = get_map().get_id(lang, args.index, id_field=args.id_field)
    print(f"index {args.index} ({lang}) -> {args.id_field}={args.id}")
    return 0


def iterJsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                raise ValueError(f"jsonl 解析失败：{path}:{line_no}") from e
            if isinstance(obj, dict):
                yield obj


def _safePathComponent(name: str) -> str:
    s = (name or "").strip().replace("\\", "/").rstrip("/")
    if not s:
        return "unknown"
    if "/" in s:
        s = s.rsplit("/", 1)[-1] or s.rsplit("/", 1)[-2]
    for ch in '<>:"|?*':
        s = s.replace(ch, "_")
    return s.replace(" ", "_") or "unknown"


def normalizeModelArgs(args: argparse.Namespace) -> None:
    """兼容旧 --model-type direct。"""
    mt = (args.model_type or "").strip().lower()
    if mt == "direct":
        print("提示: --model-type direct 已弃用，请改用 --model-type api", file=sys.stderr)
        args.model_type = "api"


def loadUnittestDb(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"找不到 unittest_db：{path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"unittest_db 格式错误（应为 dict）：{path}")
    return data


def loadLangIndex(
    data_root: str,
    lang: str,
    *,
    out_id_field: str = "index",
) -> Dict[str, Dict[str, Any]]:
    jsonl_path = os.path.join(data_root, f"{lang}.jsonl")
    if not os.path.isfile(jsonl_path):
        return {}
    index: Dict[str, Dict[str, Any]] = {}
    for line_index, obj in enumerate(iterJsonl(jsonl_path)):
        if out_id_field == "index":
            index[str(line_index)] = obj
            continue
        uid = str(obj.get(out_id_field) or "").strip()
        if uid:
            index[uid] = obj
    return index


def iterResultCodeFiles(result_dir: str) -> Iterable[Tuple[str, str, str]]:
    """遍历 result_dir/<语言>/<bug_code_uid>.txt，跳过 .error.txt。"""
    if not os.path.isdir(result_dir):
        return
    for lang in sorted(os.listdir(result_dir)):
        lang_dir = os.path.join(result_dir, lang)
        if not os.path.isdir(lang_dir):
            continue
        for name in sorted(os.listdir(lang_dir)):
            if not name.endswith(".txt") or name.endswith(".error.txt"):
                continue
            bug_uid = name[: -len(".txt")]
            if not bug_uid:
                continue
            yield lang, bug_uid, os.path.join(lang_dir, name)


def parseLangAllowlist(langs: str) -> Optional[set[str]]:
    raw = (langs or "").strip()
    if not raw:
        return None
    allow = {s.strip() for s in raw.split(",") if s.strip()}
    return allow or None


def eval(
    result_dir: str,
    *,
    data_root: str = "datasets/xcodeeval/sub_test",
    unittest_path: str = "datasets/xcodeeval/unittest_db.json",
    out_id_field: str = "index",
    report_path: str = "",
    resume: bool = False,
    max_items: int = 0,
    run_timeout: Optional[int] = None,
    compile_timeout: Optional[int] = None,
    verbose: bool = False,
    langs: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    扫描 result_dir 下已生成的修复代码，用 unittest_db 逐条评测。

    result_dir 结构：result_dir/<语言>/<bug_code_uid>.txt
    返回汇总 dict，并写入 eval_report.json。
    """
    result_dir = os.path.abspath(result_dir)
    data_root = os.path.abspath(data_root)
    report_path = (report_path or "").strip() or os.path.join(result_dir, "eval_report.json")

    unittest_db = loadUnittestDb(unittest_path)
    lang_index_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def get_item(lang: str, bug_uid: str) -> Optional[Dict[str, Any]]:
        if lang not in lang_index_cache:
            lang_index_cache[lang] = loadLangIndex(data_root, lang, out_id_field=out_id_field)
        return lang_index_cache[lang].get(bug_uid)

    prior: Dict[str, Dict[str, Any]] = {}
    if resume and os.path.isfile(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            for row in old.get("items") or []:
                key = f"{row.get('lang')}/{row.get('bug_code_uid')}"
                if key:
                    prior[key] = row
        except Exception:
            prior = {}

    tasks = list(iterResultCodeFiles(result_dir))
    if langs:
        tasks = [t for t in tasks if t[0] in langs]
    if max_items > 0:
        tasks = tasks[:max_items]

    items: List[Dict[str, Any]] = []
    by_lang: Dict[str, Dict[str, int]] = {}
    summary = {
        "total": 0,
        "pass": 0,
        "fail": 0,
        "skip": 0,
    }

    iterator: Iterable[Tuple[str, str, str]] = tasks
    pbar = None
    if tqdm is not None and tasks:
        pbar = tqdm(tasks, desc="score", unit="file")

    def _lang_bucket(lang: str) -> Dict[str, int]:
        return by_lang.setdefault(lang, {"total": 0, "pass": 0, "fail": 0, "skip": 0})

    for lang, bug_uid, code_path in iterator if pbar is None else pbar:
        key = f"{lang}/{bug_uid}"
        bucket = _lang_bucket(lang)
        bucket["total"] += 1
        summary["total"] += 1

        if resume and key in prior and prior[key].get("status") == "pass":
            row = dict(prior[key])
            row["skipped_resume"] = True
            items.append(row)
            summary["skip"] += 1
            bucket["skip"] += 1
            continue

        try:
            with open(code_path, "r", encoding="utf-8") as f:
                code = f.read()
        except Exception as e:
            row = {
                "lang": lang,
                "bug_code_uid": bug_uid,
                "code_path": code_path,
                "status": "read_error",
                "ok": False,
                "message": repr(e),
            }
            items.append(row)
            summary["fail"] += 1
            bucket["fail"] += 1
            if verbose and pbar is not None:
                pbar.write(f"[skip] {key} read_error")
            continue

        if not code.strip():
            row = {
                "lang": lang,
                "bug_code_uid": bug_uid,
                "code_path": code_path,
                "status": "empty_code",
                "ok": False,
                "message": "empty result file",
            }
            items.append(row)
            summary["fail"] += 1
            bucket["fail"] += 1
            if verbose and pbar is not None:
                pbar.write(f"[fail] {key} empty_code")
            continue

        meta = get_item(lang, bug_uid)
        if meta is None:
            row = {
                "lang": lang,
                "bug_code_uid": bug_uid,
                "code_path": code_path,
                "status": "unknown_id",
                "ok": False,
                "message": f"not in {data_root}/{lang}.jsonl",
            }
            items.append(row)
            summary["fail"] += 1
            bucket["fail"] += 1
            if verbose and pbar is not None:
                pbar.write(f"[skip] {key} unknown_id")
            continue

        src_uid = str(meta.get("src_uid") or "").strip()
        testcases = unittest_db.get(src_uid) or []
        language = str(meta.get("lang_cluster") or lang)

        run_out = run_testcases(
            language,
            code,
            testcases,
            compile_timeout=compile_timeout,
            run_timeout=run_timeout,
        )

        row = {
            "lang": lang,
            "index": bug_uid if out_id_field == "index" else None,
            "bug_code_uid": str(meta.get("bug_code_uid") or "").strip() or bug_uid,
            "src_uid": src_uid,
            "code_path": code_path,
            "language": language,
            "ok": bool(run_out.get("ok")),
            "status": run_out.get("status"),
            "message": run_out.get("message"),
            "passed": run_out.get("passed"),
            "total_cases": run_out.get("total"),
            "fail_case_index": run_out.get("fail_case_index"),
            "expected": run_out.get("expected"),
            "actual": run_out.get("actual"),
        }
        items.append(row)

        if row["ok"]:
            summary["pass"] += 1
            bucket["pass"] += 1
        else:
            summary["fail"] += 1
            bucket["fail"] += 1
            if verbose and pbar is not None:
                pbar.write(f"[fail] {key} {row['status']}")

    if pbar is not None:
        pbar.close()

    total_scored = summary["pass"] + summary["fail"]
    pass_rate = (summary["pass"] / total_scored) if total_scored > 0 else 0.0

    by_lang_report: Dict[str, Dict[str, Any]] = {}
    for lang in sorted(by_lang.keys()):
        st = by_lang[lang]
        scored = int(st.get("pass", 0)) + int(st.get("fail", 0))
        lang_rate = (int(st.get("pass", 0)) / scored) if scored > 0 else 0.0
        by_lang_report[lang] = {
            "total_files": int(st.get("total", 0)),
            "pass": int(st.get("pass", 0)),
            "fail": int(st.get("fail", 0)),
            "skip": int(st.get("skip", 0)),
            "scored": scored,
            "pass_rate": lang_rate,
        }

    report = {
        "result_dir": result_dir,
        "data_root": data_root,
        "unittest_path": unittest_path,
        "summary": {
            **summary,
            "scored": total_scored,
            "pass_rate": pass_rate,
        },
        "by_lang": by_lang_report,
        "items": items,
    }

    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n=== 评测完成: {result_dir} ===")
    print(f"报告: {report_path}")
    print(f"总计: {summary['total']} 个文件  已跑: {total_scored}  通过: {summary['pass']}  未过: {summary['fail']}  跳过: {summary['skip']}")
    if total_scored > 0:
        print(f"总通过率: {pass_rate:.2%}\n")
    print("各语言通过率:")
    print(f"{'语言':<14} {'通过':>6} {'未过':>6} {'已评测':>8} {'通过率':>10}")
    print("-" * 48)
    for lang in sorted(by_lang_report.keys()):
        st = by_lang_report[lang]
        print(
            f"{lang:<14} {st['pass']:>6} {st['fail']:>6} {st['scored']:>8} {st['pass_rate']:>9.2%}"
        )

    return report


def resolveResultDir(args: argparse.Namespace) -> str:
    """
    结果目录：<results_root>/<范式>_<模型>/ ，其下再按语言分子目录。
    """
    explicit = (getattr(args, "result_dir", None) or "").strip()
    if explicit:
        return explicit
    root = (getattr(args, "results_root", None) or "results").strip() or "results"
    paradigm = (getattr(args, "paradigm", None) or "").strip() or args.model_type
    model_slug = _safePathComponent(args.model)
    return os.path.join(root, f"{paradigm}_{model_slug}")


def _safe_lang_dir(lang_cluster: str, *, data_path: str) -> str:
    display = _display_lang(data_path=data_path, lang_cluster=lang_cluster, safe_lang="unknown")
    return display.replace("/", "_").replace("\\", "_").strip() or "unknown"


def _out_uid_for_item(
    item: Dict[str, Any],
    *,
    uid: str,
    out_id_field: str,
    line_index: Optional[int] = None,
) -> str:
    field = (out_id_field or "").strip()
    if field == "index":
        if line_index is None:
            raise ValueError("index 文件名需要 line_index")
        return str(line_index)
    if field:
        return str(item.get(field) or "").strip() or uid
    return uid


def langStatusForDataFile(
    data_path: str,
    *,
    result_dir: str,
    id_field: str,
    out_id_field: str,
    id_value: Optional[str] = None,
) -> Dict[str, Any]:
    lang = os.path.splitext(os.path.basename(data_path))[0] or "unknown"
    total = 0
    done = 0
    for line_index, obj in enumerate(iterJsonl(data_path)):
        uid = str(obj.get(id_field) or "").strip()
        if not uid:
            continue
        if id_value and uid != id_value:
            continue
        buggy = obj.get("bug_source_code") or ""
        if not isinstance(buggy, str) or not buggy.strip():
            continue
        total += 1
        lang_cluster = str(obj.get("lang_cluster") or "")
        safe_lang = _safe_lang_dir(lang_cluster, data_path=data_path)
        out_uid = _out_uid_for_item(obj, uid=uid, out_id_field=out_id_field, line_index=line_index)
        out_path = os.path.join(result_dir, safe_lang, f"{out_uid}.txt")
        if os.path.exists(out_path):
            done += 1
    pending = max(0, total - done)
    if total <= 0:
        state = "empty"
    elif done >= total:
        state = "done"
    elif done <= 0:
        state = "pending"
    else:
        state = "partial"
    return {
        "lang": lang,
        "data_path": data_path,
        "total": total,
        "done": done,
        "pending": pending,
        "state": state,
    }


def collectLangStatuses(
    data_dir: str,
    *,
    result_dir: str,
    id_field: str,
    out_id_field: str,
    id_value: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pattern = os.path.join(data_dir, "*.jsonl")
    paths = sorted(glob.glob(pattern))
    return [
        langStatusForDataFile(
            p,
            result_dir=result_dir,
            id_field=id_field,
            out_id_field=out_id_field,
            id_value=id_value,
        )
        for p in paths
    ]


def _format_lang_line(st: Dict[str, Any]) -> str:
    pct = (st["done"] / st["total"] * 100.0) if st["total"] > 0 else 0.0
    return f"  - {st['lang']}: {st['done']}/{st['total']} ({pct:5.1f}%)"


def printLangStatusSummary(
    statuses: List[Dict[str, Any]],
    *,
    title: str,
    result_dir: str,
    data_dir: str,
    current_lang: str = "",
) -> None:
    done_list = [s for s in statuses if s["state"] == "done"]
    partial_list = [s for s in statuses if s["state"] == "partial"]
    pending_list = [s for s in statuses if s["state"] == "pending"]
    empty_list = [s for s in statuses if s["state"] == "empty"]

    print(f"\n=== {title} ===")
    print(f"数据目录: {data_dir}")
    print(f"结果目录: {result_dir}")
    if current_lang:
        print(f"当前语言: {current_lang}")

    print(f"已完成 ({len(done_list)}):")
    if done_list:
        for st in done_list:
            print(_format_lang_line(st))
    else:
        print("  (无)")

    print(f"进行中 ({len(partial_list)}):")
    if partial_list:
        for st in partial_list:
            print(_format_lang_line(st))
    else:
        print("  (无)")

    print(f"未开始 ({len(pending_list)}):")
    if pending_list:
        for st in pending_list:
            print(_format_lang_line(st))
    else:
        print("  (无)")

    if empty_list:
        print(f"无有效样本 ({len(empty_list)}):")
        for st in empty_list:
            print(f"  - {st['lang']}")
    print()


def countTargetItems(path: str, *, id_value: Optional[str], id_field: str) -> int:
    n = 0
    for obj in iterJsonl(path):
        uid = str(obj.get(id_field) or "").strip()
        if not uid:
            continue
        if id_value and uid != id_value:
            continue
        buggy = obj.get("bug_source_code") or ""
        if not isinstance(buggy, str) or not buggy.strip():
            continue
        n += 1
    return n


def loadSystemPrompt(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    return file2text(path).strip()


def loadDescriptions(path: str) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for obj in iterJsonl(path):
        uid = str(obj.get("src_uid") or "").strip()
        desc = obj.get("description")
        if uid and isinstance(desc, str):
            m[uid] = desc
    return m


def buildPrompt(desc: str, buggy: str) -> str:
    # 约束输出：尽量只输出完整程序，方便直接编译运行
    return f"""Task:
{desc}

Buggy program:
{buggy}

Only output the complete corrected program code. Do not include any explanation."""


# 记录最近一次进度条状态，便于 heartbeat 打印后“重画”进度条
_LAST_PROGRESS: Dict[str, Any] = {}


def callLlmWithTimeout(chat_fn, prompt: str, *, timeout_s: int, heartbeat_s: int) -> str:
    box: Dict[str, Any] = {"ok": False, "val": None, "err": None}

    def run() -> None:
        try:
            box["val"] = chat_fn(prompt)
            box["ok"] = True
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()

    start = time.time()
    next_hb = start + heartbeat_s if heartbeat_s and heartbeat_s > 0 else None

    while True:
        t.join(timeout=0.2)
        if not t.is_alive():
            break
        now = time.time()
        if timeout_s > 0 and (now - start) >= timeout_s:
            raise TimeoutError(f"LLM timeout after {timeout_s}s")
        if next_hb is not None and now >= next_hb:
            elapsed = int(now - start)
            # 先换行打印心跳，再把进度条重画回来
            msg = f"[llm] waiting... {elapsed}s"
            if tqdm is not None:
                tqdm.write(msg)
            else:
                sys.stdout.write("\n" + msg + "\n")
            if _LAST_PROGRESS:
                _print_progress_line(**_LAST_PROGRESS)
            next_hb = now + heartbeat_s

    if box["ok"]:
        return str(box["val"] or "")
    err = box["err"]
    if err is not None:
        raise err
    raise RuntimeError("LLM call failed with unknown error")


def _render_bar(done: int, total: int, *, width: int = 24) -> str:
    if total <= 0:
        return "[?]"
    done = max(0, min(done, total))
    filled = int(width * done / total) if total else 0
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _display_lang(*, data_path: str, lang_cluster: str, safe_lang: str) -> str:
    if lang_cluster.strip():
        return lang_cluster.strip()
    base = os.path.splitext(os.path.basename(data_path))[0]
    if base:
        return base
    return safe_lang


def _tqdm_postfix(*, passed: int, failed: int, skipped: int, lang: str = "", out_uid: str = "") -> str:
    s = f"lang={lang} pass={passed} fail={failed} skip={skipped}" if lang else f"pass={passed} fail={failed} skip={skipped}"
    if out_uid:
        s += f" id={out_uid}"
    return s


def _print_progress_line(
    *,
    done: int,
    total: int,
    passed: int,
    failed: int,
    skipped: int,
    lang: str,
    now_id: str,
    suffix: str = "",
) -> None:
    pct = (done / total * 100.0) if total > 0 else 0.0
    bar = _render_bar(done, total)
    line = (
        f"\r{bar} {done}/{total} {pct:6.2f}%  lang={lang}  "
        f"pass={passed} fail={failed} skip={skipped}  id={now_id}{suffix}"
    )
    _LAST_PROGRESS.clear()
    _LAST_PROGRESS.update(
        {
            "done": done,
            "total": total,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "lang": lang,
            "now_id": now_id,
            "suffix": suffix,
        }
    )
    sys.stdout.write(line[: max(0, 200)])
    sys.stdout.flush()


def buildLlmChat(args: argparse.Namespace, system_prompt: str):
    """返回 (llm_chat, backend_label)。"""
    if args.model_type == "local":
        llm = LLM(
            LLMConfig(
                model_type="local",
                model=args.model,
                system_prompt=system_prompt,
            )
        )
        return llm.chat, "local"

    base_url = resolve_api_base_url(cli_base_url=args.base_url or "")
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("CHSHAPI_API_KEY")
    if not api_key:
        raise RuntimeError("直连需要环境变量 OPENAI_API_KEY 或 CHSHAPI_API_KEY（即 newapi 的 key）")
    llm = LLM(
        LLMConfig(
            model_type="direct",
            model=args.model,
            system_prompt=system_prompt,
            base_url=base_url,
            api_key=api_key,
        )
    )
    return llm.chat, f"api direct ({base_url})"


def mainScore(args: argparse.Namespace) -> int:
    score_dir = (args.score_dir or "").strip()
    if not score_dir:
        print("请指定 --score-dir <结果目录>", file=sys.stderr)
        return 2
    if not os.path.isdir(score_dir):
        print(f"找不到结果目录：{score_dir}", file=sys.stderr)
        return 2
    eval(
        score_dir,
        data_root=(args.data_root or "datasets/xcodeeval/sub_test").strip(),
        unittest_path=(args.tests or "datasets/xcodeeval/unittest_db.json").strip(),
        out_id_field=(args.out_id_field or "index").strip(),
        report_path=(args.score_report or "").strip(),
        resume=bool(args.resume),
        max_items=int(args.max_items or 0),
        run_timeout=int(args.run_timeout) if int(args.run_timeout or 0) > 0 else None,
        compile_timeout=int(args.compile_timeout) if int(args.compile_timeout or 0) > 0 else None,
        verbose=bool(args.print_every and args.print_every > 0),
        langs=parseLangAllowlist(getattr(args, "langs", "")),
    )
    return 0


def main() -> int:
    args = parseArgs()
    if (args.score_dir or "").strip():
        return mainScore(args)

    if not (args.data or "").strip():
        print("生成模式需要 --data；评测已有结果请用 --score-dir", file=sys.stderr)
        return 2
    if not (args.model or "").strip():
        print("生成模式需要 --model", file=sys.stderr)
        return 2

    normalizeModelArgs(args)
    if not os.path.exists(args.data):
        print(f"找不到数据文件：{args.data}", file=sys.stderr)
        return 2
    err = resolveIndexArg(args)
    if err:
        return err
    if not os.path.exists(args.descriptions):
        print(f"找不到 descriptions：{args.descriptions}", file=sys.stderr)
        return 2
    desc_map = loadDescriptions(args.descriptions)

    prompt_path = (args.prompt or "").strip() or os.path.join(_REPO_ROOT, "prompt.txt")
    system_prompt = loadSystemPrompt(prompt_path)
    if system_prompt:
        print(f"system prompt: {prompt_path}")
    else:
        print(f"警告: 未加载 system prompt（文件不存在或为空）: {prompt_path}", file=sys.stderr)

    try:
        llm_chat, backend = buildLlmChat(args, system_prompt)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 2
    print(f"LLM 后端: {backend}")

    result_dir = resolveResultDir(args)
    args.result_dir = result_dir
    print(f"结果保存目录: {result_dir}/<语言>/")

    total = 0
    passed = 0
    failed = 0
    skipped = 0

    os.makedirs(result_dir, exist_ok=True)

    target_total = countTargetItems(args.data, id_value=args.id, id_field=args.id_field)
    processed = 0
    data_file_lang = os.path.splitext(os.path.basename(args.data))[0] or "unknown"
    lang_summary_dir = (args.lang_summary_dir or "").strip() or os.path.dirname(os.path.abspath(args.data))
    lang_statuses: List[Dict[str, Any]] = []
    if not args.no_lang_summary and os.path.isdir(lang_summary_dir):
        lang_statuses = collectLangStatuses(
            lang_summary_dir,
            result_dir=args.result_dir,
            id_field=args.id_field,
            out_id_field=(args.out_id_field or "").strip(),
            id_value=args.id,
        )
        printLangStatusSummary(
            lang_statuses,
            title="语言进度（开始前）",
            result_dir=args.result_dir,
            data_dir=lang_summary_dir,
            current_lang=data_file_lang,
        )

    use_tqdm = (tqdm is not None) and (not args.no_tqdm)
    pbar = None
    if use_tqdm:
        pbar = tqdm(total=target_total, dynamic_ncols=True, desc=data_file_lang)

    for line_index, item in enumerate(iterJsonl(args.data)):
        uid = str(item.get(args.id_field) or "").strip()
        if not uid:
            continue
        if args.id and uid != args.id:
            continue

        lang_cluster = str(item.get("lang_cluster") or "")
        safe_lang = (lang_cluster or "unknown").replace("/", "_").replace("\\", "_").strip() or "unknown"
        display_lang = _display_lang(data_path=args.data, lang_cluster=lang_cluster, safe_lang=safe_lang)
        out_id_field = (args.out_id_field or "").strip()
        out_uid = _out_uid_for_item(item, uid=uid, out_id_field=out_id_field, line_index=line_index)

        out_path = os.path.join(result_dir, safe_lang, f"{out_uid}.txt")
        if args.resume and os.path.exists(out_path):
            skipped += 1
            if pbar is not None:
                pbar.set_description_str(display_lang)
                pbar.set_postfix_str(
                    _tqdm_postfix(passed=passed, failed=failed, skipped=skipped, lang=display_lang, out_uid=out_uid)
                )
            continue
        buggy = item.get("bug_source_code") or ""
        if not isinstance(buggy, str) or not buggy.strip():
            skipped += 1
            if pbar is not None:
                pbar.set_description_str(display_lang)
                pbar.set_postfix_str(
                    _tqdm_postfix(passed=passed, failed=failed, skipped=skipped, lang=display_lang, out_uid=out_uid)
                )
            continue

        desc = desc_map.get(str(item.get("src_uid") or "").strip(), "")
        if not desc:
            # 没 description 也继续跑
            desc = ""

        if pbar is not None and (args.print_every > 0) and (processed % args.print_every == 0):
            pbar.set_description_str(display_lang)
            pbar.set_postfix_str(
                _tqdm_postfix(passed=passed, failed=failed, skipped=skipped, lang=display_lang, out_uid=out_uid)
            )

        prompt = buildPrompt(desc, buggy)
        if pbar is not None:
            pbar.set_description_str(display_lang)
            pbar.set_postfix_str(
                _tqdm_postfix(passed=passed, failed=failed, skipped=skipped, lang=display_lang, out_uid=out_uid)
            )
        else:
            _print_progress_line(
                done=processed,
                total=target_total,
                passed=passed,
                failed=failed,
                skipped=skipped,
                lang=display_lang,
                now_id=out_uid,
                suffix=" (llm...)",
            )
        err_dir = os.path.join(result_dir, safe_lang)
        os.makedirs(err_dir, exist_ok=True)
        err_path = os.path.join(err_dir, f"{out_uid}.error.txt")

        attempt = 0
        while True:
            try:
                # api=直连 LLM；local=本地 Transformers
                fixed = callLlmWithTimeout(llm_chat, prompt, timeout_s=args.llm_timeout, heartbeat_s=args.heartbeat)
                # LLM 成功后先刷新一次进度（仍在本条内）
                if pbar is None:
                    _print_progress_line(
                        done=processed,
                        total=target_total,
                        passed=passed,
                        failed=failed,
                        skipped=skipped,
                        lang=display_lang,
                        now_id=out_uid,
                        suffix=f" (llm ok, {len(fixed)} chars)",
                    )
                break
            except Exception as e:
                attempt += 1

                if "FREEAPI_PROMPT_TOO_LONG_4096" in str(e) or "4096" in str(e):
                    skipped += 1
                    processed += 1
                    msg = f"[llm] skip id={out_uid}: prompt too long (4096 limit)"
                    if pbar is not None:
                        pbar.update(1)
                        pbar.set_description_str(display_lang)
                        pbar.set_postfix_str(
                            _tqdm_postfix(
                                passed=passed,
                                failed=failed,
                                skipped=skipped,
                                lang=display_lang,
                                out_uid=out_uid,
                            )
                        )
                        pbar.write(msg)
                    elif tqdm is not None:
                        tqdm.write(msg)
                    else:
                        sys.stdout.write("\n" + msg + "\n")
                        if _LAST_PROGRESS:
                            _print_progress_line(**_LAST_PROGRESS)
                    with open(err_path, "a", encoding="utf-8") as f:
                        f.write(f"skip: prompt too long (4096 limit) err={repr(e)}\n")
                    fixed = ""
                    break
                # 打印错误（换行），并把进度条重画回来
                msg = f"[llm] error id={out_uid} attempt={attempt}: {repr(e)}"
                if pbar is not None:
                    pbar.write(msg)
                elif tqdm is not None:
                    tqdm.write(msg)
                else:
                    sys.stdout.write("\n" + msg + "\n")
                    if _LAST_PROGRESS:
                        _print_progress_line(**_LAST_PROGRESS)

                # 追加写错误日志
                with open(err_path, "a", encoding="utf-8") as f:
                    f.write(f"attempt={attempt} err={repr(e)}\n")

                # 是否继续重试
                if args.retry > 0 and attempt >= args.retry:
                    failed += 1
                    total += 1
                    processed += 1
                    if pbar is not None:
                        pbar.update(1)
                        pbar.set_description_str(display_lang)
                        pbar.set_postfix_str(
                            _tqdm_postfix(
                                passed=passed,
                                failed=failed,
                                skipped=skipped,
                                lang=display_lang,
                                out_uid=out_uid,
                            )
                        )
                    else:
                        _print_progress_line(
                            done=processed,
                            total=target_total,
                            passed=passed,
                            failed=failed,
                            skipped=skipped,
                            lang=display_lang,
                            now_id=out_uid,
                            suffix=" (give up)",
                        )
                    if args.max_items and total >= args.max_items:
                        break
                    # 放弃本条，进入下一条
                    fixed = ""  # for type checker
                    break

                if args.retry_sleep > 0:
                    time.sleep(args.retry_sleep)

        # 如果已放弃本条（fixed=="" 且达到 retry 上限），进入下一条
        if not fixed:
            if args.max_items and total >= args.max_items:
                break
            continue

        # 保存修复后的代码：result/<lang_cluster>/<id>.txt
        # 注意：有些语言我们无法本地评测（例如 C#），但仍然保存 LLM 输出。
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(fixed)

        # 只保存结果：不再本地编译/运行评测
        total += 1
        passed += 1

        processed += 1
        if pbar is not None:
            pbar.update(1)
            pbar.set_description_str(display_lang)
            pbar.set_postfix_str(
                _tqdm_postfix(passed=passed, failed=failed, skipped=skipped, lang=display_lang, out_uid=out_uid)
            )
        else:
            _print_progress_line(
                done=processed,
                total=target_total,
                passed=passed,
                failed=failed,
                skipped=skipped,
                lang=display_lang,
                now_id=out_uid,
            )

        if args.max_items and total >= args.max_items:
            break

    if pbar is not None:
        pbar.close()
    sys.stdout.write("\n")
    if lang_statuses:
        lang_statuses = collectLangStatuses(
            lang_summary_dir,
            result_dir=args.result_dir,
            id_field=args.id_field,
            out_id_field=(args.out_id_field or "").strip(),
            id_value=args.id,
        )
        printLangStatusSummary(
            lang_statuses,
            title="语言进度（本轮结束后）",
            result_dir=args.result_dir,
            data_dir=lang_summary_dir,
            current_lang=data_file_lang,
        )
    print(f"Lang: {data_file_lang}")
    print(f"Total: {total}")
    print(f"Pass: {passed}")
    print(f"Fail: {failed}")
    print(f"Skip: {skipped}")
    if total:
        print(f"Pass Rate: {passed / total:.2%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# backward-compat aliases
_parse_args = parseArgs
_iter_jsonl = iterJsonl
_count_target_items = countTargetItems
_load_descriptions = loadDescriptions
_build_prompt = buildPrompt
_call_llm_with_timeout = callLlmWithTimeout
_eval_results = eval

