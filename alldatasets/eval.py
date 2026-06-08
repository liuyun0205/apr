#!/usr/bin/env python3
"""
统一评测：挂载模型 + 数据集，生成代码并跑测例。

示例：
  conda activate py311
  CUDA_VISIBLE_DEVICES=0 python alldatasets/eval.py \\
    --dataset livecodebench \\
    --model-type local \\
    --model ~/lzh/Qwen2.5-Coder-7B-Instruct \\
    --start 0 --end 10 --resume

  python alldatasets/eval.py \\
    --dataset codeforces \\
    --dataset-path ~/lzh/datasets/codeforces \\
    --model-type api --model gpt-4o --api-key sk-...
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import utils  # noqa: E402
from LLM import LLM, LLMConfig, resolve_api_base_url  # noqa: E402
from alldatasets.loader import default_dataset_path, load_dataset  # noqa: E402

try:
    from tqdm import tqdm  # type: ignore
except ImportError:  # pragma: no cover
    tqdm = None  # type: ignore

DATASET_CHOICES = [
    "apps",
    "codecontestplus",
    "codecontests",
    "livecodebench",
    "codeforces",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="统一代码生成评测（模型 + 数据集）")
    p.add_argument(
        "--dataset",
        type=str,
        required=True,
        choices=DATASET_CHOICES,
        help="数据集名称",
    )
    p.add_argument(
        "--dataset-path",
        type=str,
        default="",
        help="数据集路径；默认 ~/lzh/datasets/<dataset>",
    )
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--end", type=int, default=None, help="结束 idx（不含）")
    p.add_argument("--max-items", type=int, default=0, help="最多评测条数（0=不限）")
    p.add_argument("--resume", action="store_true", help="跳过已有结果")
    p.add_argument(
        "--results-root",
        type=str,
        default="results",
        help="结果根目录",
    )
    p.add_argument(
        "--result-dir",
        type=str,
        default="",
        help="显式结果目录（默认自动生成）",
    )
    p.add_argument(
        "--prompt-file",
        type=str,
        default=str(_REPO_ROOT / "prompt" / "solver.txt"),
        help="system prompt 文件",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="单条测例执行超时（秒）",
    )
    p.add_argument(
        "--use-public-only",
        action="store_true",
        help="仅使用公开测例（livecodebench/codeforces 的 example/public）",
    )
    p.add_argument(
        "--dataset-rollout-io-source",
        type=str,
        default="",
        choices=["", "sample", "tests"],
        help="codecontests 评测打分测例：默认 tests=test_data.json",
    )
    p.add_argument(
        "--dataset-public-io-source",
        type=str,
        default="",
        choices=["", "sample", "tests"],
        help="codecontests Public Test（保留参数，评测走 rollout 源）",
    )

    # 模型
    p.add_argument(
        "--model-type",
        type=str,
        default="local",
        choices=["local", "api"],
    )
    p.add_argument("--model", type=str, required=True, help="模型名或本地路径")
    p.add_argument("--base-url", type=str, default="")
    p.add_argument("--api-key", type=str, default="")
    p.add_argument(
        "--gpu",
        type=str,
        default="",
        help="物理 GPU 编号，如 0 或 0,1（设置 CUDA_VISIBLE_DEVICES）",
    )
    p.add_argument("--tensor-parallel-size", type=int, default=1)
    p.add_argument("--gpu-memory-utilization", type=float, default=0.9)
    p.add_argument("--max-new-tokens", type=int, default=2048)
    p.add_argument("--temperature", type=float, default=0.7)
    p.add_argument("--retry", type=int, default=2, help="LLM 失败重试次数")
    p.add_argument("--retry-sleep", type=float, default=5.0)
    p.add_argument(
        "--score-only",
        action="store_true",
        help="只评测 result-dir 下已有代码，不调用 LLM",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="失败时打印模型输出/代码/测例对比，并写入 .raw.txt",
    )
    return p.parse_args()


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _slug(text: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", (text or "").strip())
    return s[:120] or "model"


def _resolve_result_dir(args: argparse.Namespace) -> Path:
    if args.result_dir.strip():
        return Path(args.result_dir).expanduser()
    model_slug = _slug(Path(args.model).name if "/" in args.model else args.model)
    sub = f"eval_{args.dataset}_{args.model_type}_{model_slug}"
    return Path(args.results_root).expanduser() / sub


def _build_llm(args: argparse.Namespace, system_prompt: str) -> LLM:
    return LLM(
        LLMConfig(
            model_type=args.model_type,
            model=args.model,
            model_path=args.model,
            system_prompt=system_prompt,
            base_url=resolve_api_base_url(cli_base_url=args.base_url),
            api_key=args.api_key or None,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
            tensor_parallel_size=max(1, int(args.tensor_parallel_size)),
            gpu_memory_utilization=float(args.gpu_memory_utilization),
        )
    )


def _problem_id(ds, idx: int) -> str:
    try:
        row = ds.get(idx)
        for key in ("id", "task_id", "question_id", "src_uid", "bug_code_uid"):
            if key in row and str(row[key]).strip():
                return str(row[key])
    except Exception:
        pass
    return str(idx)


def _load_dataset(args: argparse.Namespace):
    path = (args.dataset_path or "").strip() or default_dataset_path(args.dataset)
    extra: Dict[str, Any] = {}
    if args.dataset in ("livecodebench",):
        extra["include_public"] = True
        extra["include_private"] = not args.use_public_only
    if args.dataset in ("codeforces",):
        extra["use_public"] = True
        extra["use_private"] = not args.use_public_only
    if args.dataset in ("codecontests",):
        if args.dataset_rollout_io_source:
            extra["rollout_io_source"] = args.dataset_rollout_io_source
        if args.dataset_public_io_source:
            extra["public_io_source"] = args.dataset_public_io_source
    return load_dataset(args.dataset, path, **extra)


def _result_paths(result_dir: Path, pid: str) -> Tuple[Path, Path, Path, Path]:
    # CodeForces id 形如 2063/A，须 slug 化以免被当成子目录
    fname = _slug(pid)
    code_path = result_dir / f"{fname}.py"
    meta_path = result_dir / f"{fname}.json"
    err_path = result_dir / f"{fname}.error.txt"
    raw_path = result_dir / f"{fname}.raw.txt"
    return code_path, meta_path, err_path, raw_path


def _failure_debug_fields(
    *,
    raw_response: str,
    code: str,
    inputs: List[str],
    outputs: List[str],
    eval_result: Dict[str, Any],
) -> Dict[str, Any]:
    failed_case = eval_result.get("failed_case")
    case_results = eval_result.get("case_results") or []
    fail_info: Dict[str, Any] = {}
    if isinstance(failed_case, int) and 0 <= failed_case < len(case_results):
        fail_info = case_results[failed_case]

    inp_preview = ""
    exp_preview = ""
    if isinstance(failed_case, int):
        if 0 <= failed_case < len(inputs):
            inp_preview = (inputs[failed_case] or "")[:500]
        if 0 <= failed_case < len(outputs):
            exp_preview = (outputs[failed_case] or "")[:500]

    return {
        "raw_response_preview": (raw_response or "")[:3000],
        "code_preview": (code or "")[:3000],
        "failed_case": failed_case,
        "fail_stderr": fail_info.get("stderr", ""),
        "fail_stdout": fail_info.get("stdout_preview", ""),
        "input_preview": inp_preview,
        "expected_preview": exp_preview,
    }


def _debug_print_failure(
    *,
    idx: int,
    pid: str,
    reason: str,
    debug: Dict[str, Any],
) -> None:
    print(f"\n[debug] idx={idx} id={pid} reason={reason}", flush=True)
    if debug.get("platform"):
        print(f"  platform={debug.get('platform')}", flush=True)
    if debug.get("starter_code_preview"):
        print(f"  starter_code={debug['starter_code_preview']!r}", flush=True)
    print(f"  raw[:800]={debug.get('raw_response_preview', '')[:800]!r}", flush=True)
    print(f"  code[:800]={debug.get('code_preview', '')[:800]!r}", flush=True)
    if debug.get("input_preview") or debug.get("expected_preview"):
        print(f"  input={debug.get('input_preview', '')!r}", flush=True)
        print(f"  expected={debug.get('expected_preview', '')!r}", flush=True)
        print(f"  stdout={debug.get('fail_stdout', '')!r}", flush=True)
        print(f"  stderr={debug.get('fail_stderr', '')!r}", flush=True)


def _is_done(meta_path: Path, code_path: Path) -> bool:
    return meta_path.is_file() and code_path.is_file()


def _evaluate_code(
    code: str,
    inputs: List[str],
    outputs: List[str],
    *,
    timeout: int,
) -> Dict[str, Any]:
    if not inputs:
        return {
            "passed": False,
            "total_cases": 0,
            "passed_cases": 0,
            "reason": "no_test_cases",
            "case_results": [],
        }

    case_results: List[Dict[str, Any]] = []
    passed_cases = 0
    for i, (inp, exp) in enumerate(zip(inputs, outputs)):
        stdout, stderr = utils.run_solve_plain(code, inp, timeout=timeout)
        ok = utils.run_solve_ok(stderr) and utils.outputs_match(stdout, exp)
        if ok:
            passed_cases += 1
        case_results.append(
            {
                "case": i,
                "passed": ok,
                "stderr": stderr,
                "stdout_preview": (stdout or "")[:500],
                "expected_preview": (exp or "")[:500],
                "input_preview": (inp or "")[:500],
            }
        )
        if not ok:
            return {
                "passed": False,
                "total_cases": len(inputs),
                "passed_cases": passed_cases,
                "reason": "wrong_answer" if utils.run_solve_ok(stderr) else "runtime_error",
                "failed_case": i,
                "case_results": case_results,
            }

    return {
        "passed": True,
        "total_cases": len(inputs),
        "passed_cases": passed_cases,
        "reason": "accepted",
        "case_results": case_results,
    }


def _chat_with_retry(llm: LLM, question: str, *, retries: int, sleep_s: float) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(max(1, retries + 1)):
        try:
            return llm.chat(question).strip()
        except Exception as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(sleep_s)
    raise RuntimeError(f"LLM 调用失败: {last_err!r}")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.model_type == "local":
        _apply_gpu_env(args.gpu)

    ds = _load_dataset(args)
    result_dir = _resolve_result_dir(args)
    result_dir.mkdir(parents=True, exist_ok=True)

    system_prompt = utils.file2text(args.prompt_file)
    llm: Optional[LLM] = None
    if not args.score_only:
        llm = _build_llm(args, system_prompt)

    n_total = len(ds.df) if hasattr(ds, "df") else 0
    end = args.end if args.end is not None else n_total
    indices = list(range(args.start, end))
    if args.max_items > 0:
        indices = indices[: args.max_items]

    passed = 0
    failed = 0
    skipped = 0
    no_cases = 0

    iterator = indices
    if tqdm is not None:
        iterator = tqdm(indices, desc=f"eval:{args.dataset}", unit="problem")

    report_path = result_dir / "eval_report.json"
    summary: Dict[str, Any] = {
        "dataset": args.dataset,
        "dataset_path": args.dataset_path or default_dataset_path(args.dataset),
        "model": args.model,
        "model_type": args.model_type,
        "score_only": args.score_only,
        "start": args.start,
        "end": end,
        "results": [],
    }

    reason_counts: Dict[str, int] = {}

    for idx in iterator:
        pid = _problem_id(ds, idx)
        code_path, meta_path, err_path, raw_path = _result_paths(result_dir, pid)

        if args.resume and _is_done(meta_path, code_path):
            skipped += 1
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
                if old.get("passed"):
                    passed += 1
                else:
                    failed += 1
            except Exception:
                pass
            continue

        question = ds.get_by_tag("description", idx)
        inputs = ds.get_io_inputs(idx, max_count=0)
        outputs = ds.get_io_outputs(idx, max_count=0)

        if not inputs or len(inputs) != len(outputs):
            no_cases += 1
            rec = {
                "idx": idx,
                "id": pid,
                "passed": False,
                "reason": "no_test_cases",
                "total_cases": 0,
            }
            if args.debug:
                extra: Dict[str, Any] = {}
                for tag in ("platform", "starter_code"):
                    try:
                        val = ds.get_by_tag(tag, idx)
                        if val:
                            extra[tag] = str(val)
                    except Exception:
                        pass
                if extra.get("starter_code"):
                    extra["starter_code_preview"] = str(extra["starter_code"])[:500]
                rec.update(extra)
                _debug_print_failure(
                    idx=idx,
                    pid=pid,
                    reason="no_test_cases",
                    debug=extra,
                )
            _write_json(meta_path, rec)
            failed += 1
            reason_counts["no_test_cases"] = reason_counts.get("no_test_cases", 0) + 1
            summary["results"].append(rec)
            continue

        raw_response = ""
        code = ""
        llm_error = ""

        if args.score_only:
            if not code_path.is_file():
                skipped += 1
                continue
            code = code_path.read_text(encoding="utf-8")
        else:
            assert llm is not None
            try:
                raw_response = _chat_with_retry(
                    llm,
                    question,
                    retries=args.retry,
                    sleep_s=args.retry_sleep,
                )
                code = utils.clean_code(raw_response)
                code_path.write_text(code, encoding="utf-8")
                if args.debug and raw_response:
                    raw_path.write_text(raw_response, encoding="utf-8")
                if err_path.is_file():
                    err_path.unlink()
            except Exception as e:
                llm_error = repr(e)
                err_path.write_text(llm_error, encoding="utf-8")
                rec = {
                    "idx": idx,
                    "id": pid,
                    "passed": False,
                    "reason": "llm_error",
                    "error": llm_error,
                }
                _write_json(meta_path, rec)
                failed += 1
                reason_counts["llm_error"] = reason_counts.get("llm_error", 0) + 1
                summary["results"].append(rec)
                if args.debug:
                    print(f"\n[debug] idx={idx} id={pid} reason=llm_error", flush=True)
                    print(f"  error={llm_error}", flush=True)
                continue

        eval_result = _evaluate_code(
            code,
            inputs,
            outputs,
            timeout=args.timeout,
        )
        rec = {
            "idx": idx,
            "id": pid,
            "passed": bool(eval_result["passed"]),
            "reason": eval_result.get("reason"),
            "total_cases": eval_result.get("total_cases", 0),
            "passed_cases": eval_result.get("passed_cases", 0),
            "failed_case": eval_result.get("failed_case"),
            "code_path": str(code_path),
            "question_len": len(question),
            "raw_response_len": len(raw_response),
        }
        reason = str(eval_result.get("reason") or "unknown")
        if not eval_result["passed"]:
            rec.update(
                _failure_debug_fields(
                    raw_response=raw_response,
                    code=code,
                    inputs=inputs,
                    outputs=outputs,
                    eval_result=eval_result,
                )
            )
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if args.debug:
                _debug_print_failure(
                    idx=idx,
                    pid=pid,
                    reason=reason,
                    debug=rec,
                )
        _write_json(meta_path, rec)
        summary["results"].append(rec)

        if eval_result["passed"]:
            passed += 1
        else:
            failed += 1

    evaluated = passed + failed
    acc = (passed / evaluated) if evaluated else 0.0
    summary.update(
        {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "no_cases": no_cases,
            "evaluated": evaluated,
            "pass_rate": acc,
            "result_dir": str(result_dir),
            "reason_counts": reason_counts,
        }
    )
    _write_json(report_path, summary)

    print(
        f"\n[summary] dataset={args.dataset} evaluated={evaluated} "
        f"passed={passed} failed={failed} skipped={skipped} "
        f"no_cases={no_cases} pass_rate={acc:.2%}"
    )
    if reason_counts:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(reason_counts.items()))
        print(f"[reasons] {parts}")
    print(f"[report] {report_path}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
