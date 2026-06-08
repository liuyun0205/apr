#!/usr/bin/env python3
"""
根据 prompt/solver.txt 生成代码，run_solve 跑测例，并与官方 output 对比。

示例（APPS 第 0 题 + 第 0 条测例）:
  CUDA_VISIBLE_DEVICES=0 python test.py \\
    --model_path ~/lzh/Qwen2.5-Coder-7B-Instruct \\
    --dataset_path ~/lzh/datasets/APPS/train --idx 0 --input_idx 0

跑一题全部测例:
  python test.py --code_file out.py \\
    --dataset_path ~/lzh/datasets/APPS/train --idx 0 --all_cases
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "model"))

import utils
from agent import Agent
from alldatasets.loader import load_dataset

_PLACEHOLDER_MODEL_PATHS = frozenset({"...", "...", "/path/to/model", "<model_path>"})


def _validate_model_path(model_path: str) -> Path:
    raw = (model_path or "").strip()
    if raw in _PLACEHOLDER_MODEL_PATHS or "..." in raw:
        raise SystemExit(
            "无效的 --model_path（不能把文档占位符 '...' 当路径）。\n"
            "请改成真实目录，例如：\n"
            "  --model_path ~/lzh/Qwen2.5-Coder-7B-Instruct"
        )
    p = Path(raw).expanduser()
    if not p.is_dir():
        raise SystemExit(f"模型目录不存在: {p}")
    if not (p / "config.json").exists():
        raise SystemExit(
            f"目录下缺少 config.json，不像 HuggingFace 模型: {p}\n"
            "请确认 --model_path 指向已下载的模型根目录。"
        )
    return p


def _normalize_output(text: str) -> str:
    return (text or "").replace("\r\n", "\n").strip()


def _outputs_match(actual: str, expected: str) -> bool:
    return _normalize_output(actual) == _normalize_output(expected)


def _load_io_pair(args, case_idx: int) -> tuple[str, str | None]:
    """返回 (input_str, expected_out|None)。"""
    if args.input is not None and case_idx > 0:
        raise SystemExit("手写 --input 时不能用 --all_cases / case_idx>0")

    if args.input is not None:
        raw = args.input
        if raw.startswith("@"):
            inp = Path(raw[1:]).expanduser().read_text(encoding="utf-8")
        else:
            inp = raw.replace("\\n", "\n")
        exp = _load_expected_manual(args)
        return inp, exp

    if args.dataset_path is not None and args.idx is not None:
        ds = load_dataset("apps", args.dataset_path)
        inputs = ds.get_io_inputs(args.idx, max_count=0)
        outputs = ds.get_io_outputs(args.idx, max_count=0)
        if not inputs:
            raise SystemExit(f"idx={args.idx} 无 input_output 测例")
        if case_idx >= len(inputs):
            raise SystemExit(
                f"case {case_idx} 超出 inputs 范围（共 {len(inputs)} 条）"
            )
        inp = inputs[case_idx]
        exp = outputs[case_idx] if case_idx < len(outputs) else None
        return inp, exp

    raise SystemExit("请指定 --input / --dataset_path + --idx")


def _load_expected_manual(args) -> str | None:
    if args.expected is not None:
        raw = args.expected
        if raw.startswith("@"):
            return Path(raw[1:]).expanduser().read_text(encoding="utf-8")
        return raw.replace("\\n", "\n")
    if args.expected_file:
        return Path(args.expected_file).expanduser().read_text(encoding="utf-8")
    return None


def _case_indices(args) -> list[int]:
    if args.all_cases:
        if args.dataset_path is None or args.idx is None:
            raise SystemExit("--all_cases 需要 --dataset_path 与 --idx")
        ds = load_dataset("apps", args.dataset_path)
        n = len(ds.get_io_inputs(args.idx, max_count=0))
        if n == 0:
            raise SystemExit(f"idx={args.idx} 无测例")
        return list(range(n))
    return [args.input_idx]


def _load_question(args) -> str:
    if args.question:
        return args.question.strip()
    if args.question_file:
        return Path(args.question_file).expanduser().read_text(encoding="utf-8").strip()
    if args.dataset_path is not None and args.idx is not None:
        ds = load_dataset("apps", args.dataset_path)
        return ds.get_by_tag("description", args.idx)
    raise SystemExit("请指定 --question / --question_file 或 --dataset_path + --idx")


def _run_one_case(
    code: str,
    input_str: str,
    expected: str | None,
    *,
    timeout: int,
    case_idx: int,
) -> bool:
    print(f"\n=== case {case_idx} ===")
    print(f"[run_solve] input_len={len(input_str)} timeout={timeout}s")
    print("--- input (repr) ---")
    print(repr(input_str[:500] + ("..." if len(input_str) > 500 else "")))

    stdout, stderr = utils.run_solve_plain(
        code,
        input_str,
        timeout=timeout,
    )
    actual = _normalize_output(stdout)

    print("--- actual stdout ---")
    print(stdout)
    print("--- stderr ---")
    print(stderr or "(empty)")

    if expected is None:
        print("[compare] 无官方 output，跳过对比")
        return True

    exp_norm = _normalize_output(expected)
    ok = _outputs_match(actual, expected)
    print("--- expected stdout ---")
    print(expected)
    print(f"[compare] {'PASS' if ok else 'FAIL'}")
    if not ok:
        print("--- diff hint ---")
        print(f"actual lines={len(actual.splitlines())} expected lines={len(exp_norm.splitlines())}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="solver 生成 + run_solve + 与官方 output 对比"
    )
    parser.add_argument("--model_path", help="HF 模型路径（--code_file 时可省略）")
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--max_new_tokens", type=int, default=2048)
    parser.add_argument("--temperature", type=float, default=0.7)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--dataset_path", help="APPS train 目录")
    parser.add_argument("--idx", type=int, default=0, help="题目 idx")
    parser.add_argument("--input_idx", type=int, default=0, help="第几条测例")
    parser.add_argument(
        "--all_cases",
        action="store_true",
        help="跑该题 input_output 中全部测例并分别对比",
    )
    parser.add_argument("--input", help='测例字符串，或 @path/to/input.txt')
    parser.add_argument("--expected", help='期望输出，或 @path/to/output.txt')
    parser.add_argument("--expected_file", help="期望输出文件路径")
    parser.add_argument("--question", help="题面（覆盖 dataset）")
    parser.add_argument("--question_file")
    parser.add_argument("--code_file", help="已有 Python 代码，跳过模型生成")
    parser.add_argument("--save_code", help="将生成代码写入该路径")
    parser.add_argument(
        "--prompt_file",
        default=str(ROOT / "prompt" / "solver.txt"),
        help="system prompt 路径",
    )
    args = parser.parse_args()

    system_prompt = utils.file2text(args.prompt_file)

    if args.code_file:
        code = Path(args.code_file).expanduser().read_text(encoding="utf-8")
        code = utils.clean_code(code)
        print(f"[skip generate] 使用代码: {args.code_file}")
    else:
        if not args.model_path:
            parser.error("未指定 --code_file 时必须提供 --model_path")
        model_dir = _validate_model_path(args.model_path)
        question = _load_question(args)
        print(f"[generate] model={model_dir} device={args.device}")
        agent = Agent(
            model_path=str(model_dir),
            system_prompt=system_prompt,
            device=args.device,
            trainable=False,
            use_lora=False,
        )
        code = agent.chat(
            question,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
        )
        code = utils.clean_code(code)
        print("--- generated code ---")
        print(code)
        print("--- end code ---")

    if args.save_code:
        out = Path(args.save_code).expanduser()
        out.write_text(code, encoding="utf-8")
        print(f"[saved] {out}")

    cases = _case_indices(args)
    passed = 0
    compared = 0
    for ci in cases:
        input_str, expected = _load_io_pair(args, ci)
        ok = _run_one_case(
            code,
            input_str,
            expected,
            timeout=args.timeout,
            case_idx=ci,
        )
        if expected is not None:
            compared += 1
            if ok:
                passed += 1

    if compared:
        print(f"\n[summary] {passed}/{compared} passed")
        if passed < compared:
            sys.exit(1)


if __name__ == "__main__":
    main()
