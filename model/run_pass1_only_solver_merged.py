#!/usr/bin/env python3
"""
only_solver LoRA：先合并（若无），再用 vLLM 评 pass@1。

  cd ~/lzh/apr
  python model/run_pass1_only_solver_merged.py

环境变量：
  GPU=7
  BASE_MODEL=~/lzh/Qwen2.5-Coder-7B-Instruct
  LORA_PATH=~/lzh/apr/outputs/only_solver/final
  MERGED_MODEL=~/lzh/apr/outputs/only_solver/merged_model
  FORCE_MERGE=1   # 强制重新合并
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

GPU = os.getenv("GPU", "7")
os.environ["CUDA_VISIBLE_DEVICES"] = GPU

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

BASE = os.path.expanduser(
    os.getenv("BASE_MODEL", "~/lzh/Qwen2.5-Coder-7B-Instruct")
)
LORA = os.path.expanduser(
    os.getenv("LORA_PATH", "~/lzh/apr/outputs/only_solver/final")
)
MERGED = os.path.expanduser(
    os.getenv("MERGED_MODEL", "~/lzh/apr/outputs/only_solver/merged_model")
)

merged_path = Path(MERGED)
need_merge = (
    os.getenv("FORCE_MERGE", "").strip() in ("1", "true", "yes")
    or not (merged_path / "config.json").exists()
)

if need_merge:
    merge_cmd = [
        sys.executable,
        "model/merge_lora.py",
        "--gpu", GPU,
        "--base-model", BASE,
        "--lora", LORA,
        "--output", MERGED,
    ]
    if os.getenv("FORCE_MERGE", "").strip() in ("1", "true", "yes"):
        merge_cmd.append("--force")
    print("合并 LoRA →", MERGED, flush=True)
    subprocess.run(merge_cmd, check=True)
else:
    print("使用已有合并模型:", MERGED, flush=True)

from eval_pass1 import main  # noqa: E402

if __name__ == "__main__":
    main([
        "--gpu", GPU,
        "--model", MERGED,
        "--dataset_path", os.path.expanduser("~/lzh/datasets/APPS/test"),
        "--roles", "solver",
        "--output-dir", "outputs/pass1_eval",
        "--gen-batch-size", "32",
        "--resume",
    ])
