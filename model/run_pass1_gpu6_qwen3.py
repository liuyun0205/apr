#!/usr/bin/env python3
"""
tmux 会话 2：物理卡 6，Qwen3-8b，跑 naive + solver pass@1。

  cd ~/lzh/apr
  python model/run_pass1_gpu6_qwen3.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# 必须在 import vLLM 之前设置
os.environ["CUDA_VISIBLE_DEVICES"] = "6"

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

from eval_pass1 import main  # noqa: E402

if __name__ == "__main__":
    main([
        "--gpu", "6",
        "--model", os.path.expanduser("~/lzh/Qwen3-8b"),
        "--dataset_path", os.path.expanduser("~/lzh/datasets/APPS/test"),
        "--roles", "naive,solver",
        "--output-dir", "outputs/pass1_eval",
        "--resume",
    ])
