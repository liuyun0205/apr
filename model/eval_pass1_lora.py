#!/usr/bin/env python3
"""
LoRA solver pass@1 评测（HF + PEFT，无 vLLM）。

流程同 eval_pass1.py：生成代码 → APPS/test input 跑 → 对官方 output。

示例：
  python model/eval_pass1_lora.py \
    --gpu 7 \
    --base-model ~/lzh/Qwen2.5-Coder-7B-Instruct \
    --lora ~/lzh/apr/outputs/only_solver/final \
    --dataset_path ~/lzh/datasets/APPS/test \
    --resume
"""
from __future__ import annotations

import argparse
import gc
import os
import sys
from pathlib import Path
from typing import List, Optional

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

from agent import Agent  # noqa: E402
from eval_pass1 import ROLE_PROMPT_FILES  # noqa: E402

# 复用 eval_pass1 的评测主流程，但替换生成后端


class HFAgentBackend:
    """用 HF Agent（含 LoRA adapter）逐题生成代码。"""

    def __init__(
        self,
        *,
        base_model: str,
        lora_path: str,
        device: str,
        max_new_tokens: int,
        temperature: float,
    ):
        self._agent = Agent(
            model_path=base_model,
            system_prompt="",
            device=device,
            trainable=False,
            use_lora=False,
            lora_path=lora_path,
        )
        self._max_new_tokens = max_new_tokens
        self._temperature = temperature

    def chat_batch(self, user_contents: List[str], *, system_prompt: str = "") -> List[str]:
        self._agent.system_prompt = system_prompt
        return [
            self._agent.chat(
                q,
                max_new_tokens=self._max_new_tokens,
                temperature=self._temperature,
            )
            for q in user_contents
        ]

    def release(self) -> None:
        del self._agent.model
        self._agent.model = None
        gc.collect()
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.synchronize()
                torch.cuda.empty_cache()
        except Exception:
            pass


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LoRA solver pass@1（HF+PEFT）")
    p.add_argument("--base-model", type=str, required=True, help="基座模型路径")
    p.add_argument(
        "--lora",
        type=str,
        required=True,
        help="LoRA 目录（含 adapter_config.json），如 outputs/only_solver/final",
    )
    p.add_argument("--dataset_path", type=str, default="~/lzh/datasets/APPS/test")
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--end", type=int, default=None)
    p.add_argument(
        "--roles",
        type=str,
        default="solver",
        help="LoRA 只训了 solver，默认只评 solver",
    )
    p.add_argument("--gpu", type=str, default="", help="物理 GPU，如 7")
    p.add_argument("--device", type=str, default="cuda:0", help="可见卡内的 device")
    p.add_argument("--max-new-tokens", type=int, default=2048)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--gen-batch-size", type=int, default=8, help="HF 逐题生成，批小些更稳")
    p.add_argument("--exec-timeout", type=int, default=30)
    p.add_argument("--exec-workers", type=int, default=8)
    p.add_argument("--output-dir", type=str, default="outputs/pass1_eval")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--print-every", type=int, default=50)
    return p.parse_args(argv)


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _model_label(lora_path: str) -> str:
    p = Path(lora_path).expanduser()
    if p.name in ("final", "step_0") and p.parent.name:
        return f"{p.parent.name}_{p.name}"
    return p.name or "lora"


def run_lora_eval(args: argparse.Namespace):
    _apply_gpu_env(args.gpu)

    # 结果文件名用 only_solver_final 这类标签，避免都叫 final
    args.model = _model_label(args.lora)
    args.base_model = str(Path(args.base_model).expanduser())
    args.lora_path = str(Path(args.lora).expanduser())
    args.model_type = "local"
    if not hasattr(args, "tensor_parallel_size"):
        args.tensor_parallel_size = 1
    if not hasattr(args, "gpu_memory_utilization"):
        args.gpu_memory_utilization = 0.9
    if not hasattr(args, "base_url"):
        args.base_url = ""
    if not hasattr(args, "api_key"):
        args.api_key = ""

    from alldatasets.loader import load_dataset
    from eval_pass1 import _parse_roles, run_one_role

    ds_path = str(Path(args.dataset_path).expanduser())
    ds = load_dataset("apps", ds_path)
    print(f"加载数据集: {ds_path}，共 {len(ds.df)} 题", flush=True)
    print(
        f"LoRA 评测: base={args.base_model} adapter={args.lora} device={args.device}",
        flush=True,
    )

    backend = HFAgentBackend(
        base_model=str(Path(args.base_model).expanduser()),
        lora_path=str(Path(args.lora).expanduser()),
        device=args.device,
        max_new_tokens=args.max_new_tokens,
        temperature=args.temperature,
    )
    roles = _parse_roles(args.roles)
    summaries = []
    try:
        for role in roles:
            print(f"\n===== role={role} lora={args.lora} =====", flush=True)
            summaries.append(run_one_role(args, role, ds, backend))
    finally:
        print("释放 HF 显存...", flush=True)
        backend.release()
    return summaries


def main(argv: Optional[List[str]] = None) -> None:
    run_lora_eval(parse_args(argv))


if __name__ == "__main__":
    main()
