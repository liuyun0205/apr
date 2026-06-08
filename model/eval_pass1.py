#!/usr/bin/env python3
"""
APPS pass@1 评测：
  1. 模型生成 1 份代码（naive / solver prompt）
  2. 用 APPS/test 官方 input 跑代码
  3. 与官方 output 对比，全测例通过即 pass@1

无注入退避，超时/报错直接判失败。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402
from LLM import LLM, LLMConfig, resolve_api_base_url  # noqa: E402
from alldatasets.loader import load_dataset  # noqa: E402

try:
    from tqdm import tqdm  # noqa: E402
except ImportError:  # pragma: no cover
    tqdm = None  # type: ignore

ROLE_PROMPT_FILES = {
    "naive": "prompt/naivesolver.txt",
    "solver": "prompt/solver.txt",
}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="pass@1：naive/solver 代码生成 + 官方测例")
    p.add_argument(
        "--dataset",
        type=str,
        default="apps",
        choices=["apps", "codecontests"],
        help="数据集类型",
    )
    p.add_argument(
        "--dataset_path",
        type=str,
        default="",
        help="数据集路径；为空时用各数据集默认路径",
    )
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--end", type=int, default=None, help="结束 idx（不含）")
    p.add_argument(
        "--roles",
        type=str,
        default="naive,solver",
        help="逗号分隔：naive / solver",
    )
    p.add_argument(
        "--model",
        type=str,
        required=True,
        help="HF 模型路径，如 ~/lzh/Qwen2.5-Coder-7B-Instruct",
    )
    p.add_argument(
        "--model-type",
        type=str,
        default="local",
        choices=["local", "api"],
    )
    p.add_argument("--base-url", type=str, default="")
    p.add_argument("--api-key", type=str, default="")
    p.add_argument(
        "--gpu",
        type=str,
        default="",
        help="物理 GPU，如 5（设置 CUDA_VISIBLE_DEVICES）",
    )
    p.add_argument("--tensor-parallel-size", type=int, default=1)
    p.add_argument("--gpu-memory-utilization", type=float, default=0.9)
    p.add_argument("--max-new-tokens", type=int, default=2048)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--gen-batch-size", type=int, default=32, help="vLLM 批量生成题数")
    p.add_argument("--exec-timeout", type=int, default=30, help="单测例子进程超时（秒）")
    p.add_argument("--exec-workers", type=int, default=8, help="并行评测进程数")
    p.add_argument(
        "--output-dir",
        type=str,
        default="outputs/pass1_eval",
        help="结果目录（jsonl + summary.json）",
    )
    p.add_argument("--resume", action="store_true", help="跳过已评测 idx")
    p.add_argument("--print-every", type=int, default=50)
    return p.parse_args(argv)


def _parse_roles(raw: str) -> List[str]:
    roles = [r.strip().lower() for r in (raw or "").split(",") if r.strip()]
    bad = [r for r in roles if r not in ROLE_PROMPT_FILES]
    if bad:
        raise ValueError(f"未知 role: {bad}，可选: {list(ROLE_PROMPT_FILES)}")
    if not roles:
        raise ValueError("至少指定一个 role")
    return roles


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _model_slug(model_path: str) -> str:
    name = Path(model_path).expanduser().name or "model"
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in name)


def _result_path(output_dir: Path, role: str, slug: str) -> Path:
    return output_dir / f"{slug}_{role}.jsonl"


def _summary_path(output_dir: Path, role: str, slug: str) -> Path:
    return output_dir / f"{slug}_{role}_summary.json"


def _load_done_indices(path: Path) -> Set[int]:
    done: Set[int] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(int(json.loads(line)["idx"]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
    return done


def _append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _build_llm(args: argparse.Namespace, system_prompt: str) -> LLM:
    return LLM(
        LLMConfig(
            model_type=args.model_type,
            model=str(Path(args.model).expanduser()),
            model_path=str(Path(args.model).expanduser()),
            system_prompt=system_prompt,
            base_url=resolve_api_base_url(cli_base_url=args.base_url),
            api_key=args.api_key or None,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
            tensor_parallel_size=max(1, int(args.tensor_parallel_size)),
            gpu_memory_utilization=float(args.gpu_memory_utilization),
        )
    )


def _exec_timeout(args: argparse.Namespace) -> int:
    return int(args.exec_timeout)


def _eval_one_problem(
    code: str,
    inputs: List[str],
    outputs: List[str],
    timeout: int,
) -> Tuple[bool, int, int, str]:
    code = utils.clean_code(code)
    if not code.strip():
        return False, 0, len(inputs), "empty code"
    if not inputs:
        return False, 0, 0, "no inputs"
    if len(outputs) < len(inputs):
        return False, 0, len(inputs), "missing outputs"

    passed = 0
    last_err = ""
    for inp, exp in zip(inputs, outputs):
        stdout, stderr = utils.run_solve_plain(code, inp, timeout=timeout)
        if utils.run_solve_ok(stderr) and utils.outputs_match(stdout, exp):
            passed += 1
        else:
            last_err = stderr or "output mismatch"
            break
    return passed == len(inputs), passed, len(inputs), last_err


def _eval_worker(
    payload: Tuple[str, List[str], List[str], int],
) -> Tuple[bool, int, int, str]:
    return _eval_one_problem(*payload)


def _chunked(items: List[int], size: int) -> List[List[int]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def run_one_role(
    args: argparse.Namespace,
    role: str,
    ds,
    llm: LLM,
) -> Dict[str, Any]:
    system_prompt = utils.file2text(ROLE_PROMPT_FILES[role])
    exec_timeout = _exec_timeout(args)

    slug = _model_slug(args.model)
    out_dir = Path(args.output_dir)
    result_path = _result_path(out_dir, role, slug)
    summary_path = _summary_path(out_dir, role, slug)

    end = args.end if args.end is not None else len(ds.df)
    all_indices = list(range(args.start, end))
    done = _load_done_indices(result_path) if args.resume else set()
    pending = [i for i in all_indices if i not in done]
    print(
        f"[{role}] 数据集={len(ds.df)} 题 | 本轮范围=[{args.start},{end}) "
        f"| 已完成={len(done)} | 待跑={len(pending)} | 结果={result_path}",
        flush=True,
    )
    if len(ds.df) < 1000 and args.dataset == "apps":
        print(
            f"警告: 数据集仅 {len(ds.df)} 题，请确认 --dataset_path 指向完整 APPS/test（应有约 5000 题）",
            flush=True,
        )
    passed_n = 0
    evaluated_n = 0
    skipped_n = len(all_indices) - len(pending)
    no_io_n = 0
    empty_code_n = 0

    if args.resume and result_path.exists():
        with result_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    if row.get("skipped"):
                        continue
                    evaluated_n += 1
                    if row.get("pass_at_1"):
                        passed_n += 1
                except json.JSONDecodeError:
                    continue

    batches = _chunked(pending, max(1, args.gen_batch_size))
    iterator = batches
    if tqdm is not None:
        iterator = tqdm(batches, desc=f"{slug}/{role}", unit="batch")

    for batch in iterator:
        questions = [ds.get_by_tag("description", idx) for idx in batch]
        raw_codes = llm.chat_batch(questions, system_prompt=system_prompt)
        codes = [utils.clean_code(c) for c in raw_codes]

        eval_payloads = []
        meta = []
        for idx, raw_code, code in zip(batch, raw_codes, codes):
            pid = str(ds.get(idx).get("id", idx))
            inputs = ds.get_io_inputs(idx, max_count=0)
            outputs = ds.get_io_outputs(idx, max_count=0)
            if not inputs:
                no_io_n += 1
                record = {
                    "idx": idx,
                    "id": pid,
                    "role": role,
                    "model": args.model,
                    "skipped": True,
                    "reason": "no_inputs",
                    "pass_at_1": False,
                }
                _append_jsonl(result_path, record)
                continue
            if not code.strip():
                empty_code_n += 1
                record = {
                    "idx": idx,
                    "id": pid,
                    "role": role,
                    "model": args.model,
                    "raw_code": raw_code,
                    "code": code,
                    "num_cases": len(inputs),
                    "skipped": False,
                    "pass_at_1": False,
                    "error": "empty code",
                }
                _append_jsonl(result_path, record)
                evaluated_n += 1
                continue

            eval_payloads.append((code, inputs, outputs, exec_timeout))
            meta.append((idx, pid, raw_code, code, len(inputs)))

        if not eval_payloads:
            continue

        workers = max(1, int(args.exec_workers))
        if workers == 1:
            results = [_eval_worker(p) for p in eval_payloads]
        else:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(_eval_worker, eval_payloads))

        for (idx, pid, raw_code, code, num_cases), (ok, passed_cases, total_cases, err) in zip(
            meta, results
        ):
            evaluated_n += 1
            if ok:
                passed_n += 1
            record = {
                "idx": idx,
                "id": pid,
                "role": role,
                "model": args.model,
                "raw_code": raw_code,
                "code": code,
                "num_cases": num_cases,
                "passed_cases": passed_cases,
                "total_cases": total_cases,
                "pass_at_1": ok,
                "error": err,
            }
            _append_jsonl(result_path, record)

        if args.print_every > 0 and evaluated_n and evaluated_n % args.print_every == 0:
            rate = passed_n / evaluated_n if evaluated_n else 0.0
            msg = (
                f"[{role}] {slug} evaluated={evaluated_n} "
                f"pass@1={rate:.4f} ({passed_n}/{evaluated_n})"
            )
            if tqdm is not None:
                tqdm.write(msg)
            else:
                print(msg, flush=True)

    pass_rate = passed_n / evaluated_n if evaluated_n else 0.0
    summary = {
        "dataset_path": str(Path(args.dataset_path).expanduser()),
        "role": role,
        "model": args.model,
        "model_slug": slug,
        "base_model": getattr(args, "base_model", ""),
        "lora_path": getattr(args, "lora_path", ""),
        "start": args.start,
        "end": end,
        "evaluated": evaluated_n,
        "passed": passed_n,
        "pass_at_1": pass_rate,
        "skipped_resume": skipped_n,
        "no_io": no_io_n,
        "empty_code": empty_code_n,
        "result_file": str(result_path),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(
        f"完成 [{role}] {slug}: pass@1={pass_rate:.4f} "
        f"({passed_n}/{evaluated_n}) -> {summary_path}",
        flush=True,
    )
    return summary


def _default_dataset_path(dataset: str) -> str:
    if dataset == "codecontests":
        return str(Path.home() / "datasets/codecontests")
    return str(Path.home() / "lzh/datasets/APPS/test")


def run_eval(args: argparse.Namespace) -> List[Dict[str, Any]]:
    if args.model_type == "local":
        _apply_gpu_env(args.gpu)

    ds_name = (args.dataset or "apps").strip().lower()
    ds_path = str(
        Path(args.dataset_path or _default_dataset_path(ds_name)).expanduser()
    )
    ds = load_dataset(ds_name, ds_path)
    print(f"加载数据集: {ds_path}，共 {len(ds.df)} 题", flush=True)
    roles = _parse_roles(args.roles)

    summaries = []
    llm = _build_llm(args, system_prompt="")
    try:
        for role in roles:
            print(
                f"\n===== role={role} model={args.model} gpu={args.gpu or 'default'} =====",
                flush=True,
            )
            summaries.append(run_one_role(args, role, ds, llm))
    finally:
        print("释放 vLLM 显存...", flush=True)
        llm.release()
    return summaries


def main(argv: Optional[List[str]] = None) -> None:
    run_eval(parse_args(argv))


if __name__ == "__main__":
    main()
