#!/usr/bin/env python3
"""
逐题推理：对每道题一次生成推理链 + 代码（naive / solver）。

每道题写入数据集目录下：<题号>/apr/infer_reasoning.jsonl
例如 APPS：train/0000/apr/infer_reasoning.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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

PROMPT_FILES = {
    "naive": "prompt/naive_reasoning.txt",
    "solver": "prompt/solver_reasoning.txt",
}

OUTPUT_FILENAME = "infer_reasoning.jsonl"

_TAG_REASONING = re.compile(
    r"<reasoning>\s*(.*?)\s*</reasoning>",
    flags=re.DOTALL | re.IGNORECASE,
)
_TAG_CODE = re.compile(
    r"<code>\s*(.*?)\s*</code>",
    flags=re.DOTALL | re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="逐题推理：一次输出推理链 + naive/solver 代码（JSONL）"
    )
    p.add_argument(
        "--dataset",
        type=str,
        default="apps",
        choices=["apps", "codecontestplus"],
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
        "--output-name",
        type=str,
        default=OUTPUT_FILENAME,
        help="每题输出文件名，写在 <题号>/apr/ 下",
    )
    p.add_argument("--resume", action="store_true", help="跳过各题 apr 文件中已有的 role")
    p.add_argument(
        "--model-type",
        type=str,
        default="local",
        choices=["local", "api"],
        help="local=vLLM；api=OpenAI 兼容 HTTP",
    )
    p.add_argument(
        "--model",
        type=str,
        default="/home/liuzhihao/lzh/Qwen2.5-Coder-7B-Instruct",
    )
    p.add_argument("--base-url", type=str, default="")
    p.add_argument("--api-key", type=str, default="")
    p.add_argument(
        "--gpu",
        type=str,
        default="",
        help="物理 GPU 编号，如 5（等价 CUDA_VISIBLE_DEVICES=5）；多卡用逗号 5,6",
    )
    p.add_argument("--tensor-parallel-size", type=int, default=1)
    p.add_argument("--gpu-memory-utilization", type=float, default=0.9)
    p.add_argument("--max-new-tokens", type=int, default=3072)
    p.add_argument("--temperature", type=float, default=0.7)
    p.add_argument(
        "--print-every",
        type=int,
        default=1,
        help="每 N 题打印进度（0=不打印）",
    )
    return p.parse_args()


def _default_dataset_path(dataset: str) -> str:
    if dataset == "apps":
        return str(Path.home() / "lzh/datasets/APPS/train")
    if dataset == "codecontests":
        return str(Path.home() / "datasets/codecontests")
    return str(Path.home() / "lzh/datasets/codecontestplus")


def _parse_roles(raw: str) -> List[str]:
    roles = [r.strip().lower() for r in (raw or "").split(",") if r.strip()]
    bad = [r for r in roles if r not in PROMPT_FILES]
    if bad:
        raise ValueError(f"未知 role: {bad}，可选: {list(PROMPT_FILES)}")
    if not roles:
        raise ValueError("至少指定一个 role")
    return roles


def _load_done_roles(path: Path) -> Set[str]:
    done: Set[str] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                done.add(str(row["role"]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
    return done


def _problem_apr_path(ds, idx: int, *, filename: str) -> Path:
    problem_dir_getter = getattr(ds, "problem_dir", None)
    if callable(problem_dir_getter):
        return problem_dir_getter(idx) / "apr" / filename

    row = ds.get(idx)
    if "dir" in row and str(row["dir"]).strip():
        return Path(str(row["dir"])) / "apr" / filename

    root = Path(getattr(ds, "path", ".")).expanduser()
    return root / _problem_id(ds, idx) / "apr" / filename


def _problem_id(ds, idx: int) -> str:
    getter = getattr(ds, "get", None)
    if not callable(getter):
        return str(idx)
    try:
        row = getter(idx)
        for key in ("id", "apr_id", "src_uid", "bug_code_uid"):
            if key in row and str(row[key]).strip():
                return str(row[key])
    except Exception:
        pass
    return str(idx)


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _build_llm(args: argparse.Namespace) -> LLM:
    return LLM(
        LLMConfig(
            model_type=args.model_type,
            model=args.model,
            model_path=args.model,
            system_prompt="",
            base_url=resolve_api_base_url(cli_base_url=args.base_url),
            api_key=args.api_key or None,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
            tensor_parallel_size=max(1, int(args.tensor_parallel_size)),
            gpu_memory_utilization=float(args.gpu_memory_utilization),
        )
    )


def parse_reasoning_and_code(raw: str) -> Tuple[str, str]:
    """从模型输出中解析 <reasoning> 与 <code>；缺标签时做启发式回退。"""
    text = (raw or "").strip()
    if not text:
        return "", ""

    reasoning_m = _TAG_REASONING.search(text)
    code_m = _TAG_CODE.search(text)

    if reasoning_m and code_m:
        reasoning = reasoning_m.group(1).strip()
        code = utils.clean_code(code_m.group(1).strip())
        return reasoning, code

    if code_m:
        reasoning = _TAG_REASONING.sub("", text[: code_m.start()]).strip()
        code = utils.clean_code(code_m.group(1).strip())
        return reasoning, code

    code = utils.clean_code(text)
    if code != text.strip():
        reasoning = text[: text.lower().find("```")].strip()
        return reasoning, code

    return text, ""


def _generate(
    llm: LLM,
    role: str,
    question: str,
    prompts: Dict[str, str],
) -> Tuple[str, str, str]:
    raw = llm.chat(question, system_prompt=prompts[role]).strip()
    reasoning, code = parse_reasoning_and_code(raw)
    return raw, reasoning, code


def _append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _iter_indices(ds, start: int, end: Optional[int]) -> Iterable[int]:
    n = len(ds.df) if hasattr(ds, "df") else None
    if end is None:
        end = n if n is not None else start + 1
    return range(start, end)


def main() -> None:
    args = parse_args()
    if args.model_type == "local":
        _apply_gpu_env(args.gpu)
    roles = _parse_roles(args.roles)

    ds_path = args.dataset_path.strip() or _default_dataset_path(args.dataset)
    ds = load_dataset(args.dataset, ds_path)

    llm = _build_llm(args)
    prompts = {r: utils.file2text(PROMPT_FILES[r]) for r in roles}

    indices = list(_iter_indices(ds, args.start, args.end))
    total_tasks = len(indices) * len(roles)
    skipped = 0
    written = 0

    iterator = indices
    if tqdm is not None:
        iterator = tqdm(indices, desc="infer", unit="problem")

    for i, idx in enumerate(iterator):
        question = ds.get_by_tag("description", idx)
        pid = _problem_id(ds, idx)
        out_path = _problem_apr_path(ds, idx, filename=args.output_name)
        done_roles = _load_done_roles(out_path) if args.resume else set()

        for role in roles:
            if role in done_roles:
                skipped += 1
                continue

            raw, reasoning, code = _generate(llm, role, question, prompts)

            record = {
                "idx": idx,
                "id": pid,
                "role": role,
                "dataset": args.dataset,
                "question": question,
                "raw_response": raw,
                "reasoning": reasoning,
                "code": code,
                "model": args.model,
                "model_type": args.model_type,
                "system_prompt": prompts[role],
                "output_path": str(out_path),
            }
            _append_jsonl(out_path, record)
            done_roles.add(role)
            written += 1

        if args.print_every > 0 and (i + 1) % args.print_every == 0:
            msg = (
                f"[{i + 1}/{len(indices)}] idx={idx} id={pid} "
                f"out={out_path} written={written} skipped={skipped}"
            )
            if tqdm is not None:
                tqdm.write(msg)
            else:
                print(msg, flush=True)

    print(
        f"完成：数据集 {ds_path}，写入 {written} 条，跳过 {skipped} 条，"
        f"共 {total_tasks} 个任务槽位；每题 -> <题号>/apr/{args.output_name}",
        flush=True,
    )


if __name__ == "__main__":
    main()
