#!/usr/bin/env python3
"""
从 CodeContests+ 的 correct_submissions 构建 solver SFT 冷启动数据。

每行 jsonl：
  {"idx", "problem_id", "question", "code", "language"}

示例：
  python model/prepare_solver_sft.py \\
    --dataset_path ~/datasets/codecontestplus \\
    --output outputs/solver_sft/train.jsonl \\
    --language python \\
    --max_per_problem 1
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from pathlib import Path
from typing import Any, Dict, List

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402
from alldatasets.loader import default_dataset_path, load_dataset  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CodeContests+ correct_submissions → solver SFT jsonl"
    )
    p.add_argument(
        "--dataset_path",
        type=str,
        default="",
        help="CodeContests+ parquet 目录；默认 ~/datasets/codecontestplus",
    )
    p.add_argument(
        "--output",
        type=str,
        default="outputs/solver_sft/train.jsonl",
        help="输出 jsonl 路径",
    )
    p.add_argument(
        "--language",
        type=str,
        default="python",
        help="只保留该语言提交（子串匹配，如 python 匹配 Python/PyPy3）；空=不过滤",
    )
    p.add_argument(
        "--max_per_problem",
        type=int,
        default=1,
        help="每题最多采样几条 accepted solution",
    )
    p.add_argument(
        "--submission_pick",
        type=str,
        default="first",
        choices=["first", "random"],
        help="每题从 correct_submissions 中取法",
    )
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--end", type=int, default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--min_code_chars",
        type=int,
        default=20,
        help="代码过短则跳过",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="打印 correct_submissions 解析诊断信息",
    )
    return p.parse_args()


def _pick_submissions(
    subs: List[dict],
    *,
    max_per_problem: int,
    pick: str,
    rng: random.Random,
) -> List[dict]:
    if not subs or max_per_problem <= 0:
        return []
    if pick == "random":
        if len(subs) <= max_per_problem:
            return list(subs)
        return rng.sample(subs, max_per_problem)
    return subs[:max_per_problem]


def main() -> None:
    args = parse_args()
    path = (args.dataset_path or "").strip() or default_dataset_path("codecontestplus")
    dataset = load_dataset("codecontestplus", path)

    end = args.end if args.end is not None else len(dataset.df)
    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    n_written = 0
    skip_reasons = {
        "no_question": 0,
        "no_submission": 0,
        "code_too_short": 0,
    }

    with out.open("w", encoding="utf-8") as f:
        for idx in range(args.start, end):
            question = str(dataset.get_by_tag("description", idx) or "").strip()
            if not question:
                skip_reasons["no_question"] += 1
                continue

            problem_id = str(dataset.get_by_tag("id", idx))
            subs = dataset.get_accepted_solutions(idx, language=args.language or None)
            chosen = _pick_submissions(
                subs,
                max_per_problem=args.max_per_problem,
                pick=args.submission_pick,
                rng=rng,
            )
            if not chosen:
                skip_reasons["no_submission"] += 1
                continue

            wrote = False
            for sub in chosen:
                code = utils.clean_code(str(sub.get("code") or ""))
                if len(code.strip()) < args.min_code_chars:
                    skip_reasons["code_too_short"] += 1
                    continue
                rec = {
                    "idx": idx,
                    "problem_id": problem_id,
                    "question": question,
                    "code": code,
                    "language": str(sub.get("language") or ""),
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n_written += 1
                wrote = True
            if not wrote:
                skip_reasons["no_submission"] += 1

    meta = {
        "dataset_path": path,
        "output": str(out),
        "language_filter": args.language,
        "max_per_problem": args.max_per_problem,
        "submission_pick": args.submission_pick,
        "start": args.start,
        "end": end,
        "seed": args.seed,
        "num_records": n_written,
        "skip_reasons": skip_reasons,
    }
    diag = dataset.diagnose_submissions(start=args.start, end=end)
    if args.debug or n_written == 0:
        meta["diagnostics"] = diag
    meta_path = out.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"写入 {n_written} 条 → {out}\n"
        f"元数据 → {meta_path}\n"
        f"跳过统计 → {json.dumps(skip_reasons, ensure_ascii=False)}"
    )
    if args.debug or n_written == 0:
        print("=== correct_submissions 诊断 ===")
        print(json.dumps(diag, ensure_ascii=False, indent=2))
    if n_written == 0:
        print(
            "提示：若 parsed_nonempty=0，说明 correct_submissions 没被正确解析（常见是 JSON 字符串）；"
            "请更新代码后重试。若 python_nonempty=0，可试 --language '' 查看是否语言字段缺失。"
        )


if __name__ == "__main__":
    main()
