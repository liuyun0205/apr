#!/usr/bin/env python3
"""
将 LoRA adapter 合并进基座模型，导出完整 HF 权重供 vLLM 使用。

示例：
  cd ~/lzh/apr
  python model/merge_lora.py \
    --gpu 7 \
    --base-model ~/lzh/Qwen2.5-Coder-7B-Instruct \
    --lora ~/lzh/apr/outputs/only_solver/final \
    --output ~/lzh/apr/outputs/only_solver/merged_model
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LoRA 合并为完整 HF 模型")
    p.add_argument("--base-model", type=str, required=True, help="基座模型路径")
    p.add_argument("--lora", type=str, required=True, help="LoRA 目录（含 adapter_config.json）")
    p.add_argument(
        "--output",
        type=str,
        default="",
        help="合并后输出目录；默认 <lora父目录>/merged_model",
    )
    p.add_argument("--gpu", type=str, default="", help="物理 GPU，如 7")
    p.add_argument(
        "--device",
        type=str,
        default="cuda:0",
        help="合并时使用的 device（设 CUDA_VISIBLE_DEVICES 后一般为 cuda:0）",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="输出目录已存在时仍覆盖合并",
    )
    return p.parse_args()


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _default_output(lora_path: Path) -> Path:
    return lora_path.parent / "merged_model"


def merge_lora(
    *,
    base_model: str,
    lora_path: str,
    output_dir: str,
    device: str = "cuda:0",
    force: bool = False,
) -> Path:
    import torch
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer

    base = str(Path(base_model).expanduser())
    lora = Path(lora_path).expanduser()
    out = Path(output_dir).expanduser()

    if not (lora / "adapter_config.json").exists():
        raise FileNotFoundError(f"LoRA 目录无效，缺少 adapter_config.json: {lora}")

    if out.exists() and any(out.iterdir()):
        if (out / "config.json").exists() and not force:
            print(f"已存在合并模型，跳过: {out}", flush=True)
            return out
        if force:
            print(f"强制覆盖合并输出: {out}", flush=True)

    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    print(f"加载基座: {base}", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(base, trust_remote_code=True)
    base_model_obj = AutoModelForCausalLM.from_pretrained(
        base,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16,
        device_map=device,
    )

    print(f"加载 LoRA: {lora}", flush=True)
    peft_model = PeftModel.from_pretrained(base_model_obj, str(lora))

    print("合并权重 merge_and_unload() ...", flush=True)
    merged = peft_model.merge_and_unload()

    print(f"保存到: {out}", flush=True)
    merged.save_pretrained(str(out), safe_serialization=True)
    tokenizer.save_pretrained(str(out))

    elapsed = time.time() - t0
    print(f"完成，耗时 {elapsed:.1f}s → {out}", flush=True)
    return out


def main() -> None:
    args = parse_args()
    _apply_gpu_env(args.gpu)

    lora_p = Path(args.lora).expanduser()
    output = args.output.strip() or str(_default_output(lora_p))
    merge_lora(
        base_model=args.base_model,
        lora_path=str(lora_p),
        output_dir=output,
        device=args.device,
        force=args.force,
    )


if __name__ == "__main__":
    main()
