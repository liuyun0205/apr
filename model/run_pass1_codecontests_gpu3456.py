#!/usr/bin/env python3
"""
CodeContests 全量 pass@1：物理卡 3,4,5,6，vLLM tp=4，Qwen2.5-Coder naive+solver。

  cd ~/lzh/apr
  python model/run_pass1_codecontests_gpu3456.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# 必须在 import vLLM 之前设置
os.environ["CUDA_VISIBLE_DEVICES"] = "3,4,5,6"

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

from eval_pass1 import main  # noqa: E402

if __name__ == "__main__":
    main([
        "--gpu", "3,4,5,6",
        "--dataset", "codecontests",
        "--dataset_path", os.path.expanduser("~/datasets/codecontests"),
        "--model", os.path.expanduser("~/lzh/Qwen2.5-Coder-7B-Instruct"),
        "--tensor-parallel-size", "4",
        "--roles", "naive,solver",
        "--exec-timeout", "30",
        "--exec-workers", "8",
        "--output-dir", "outputs/pass1_eval_codecontests",
        "--start", "0",
        "--end", "9644",
        "--resume",
    ])
