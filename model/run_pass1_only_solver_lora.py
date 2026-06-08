#!/usr/bin/env python3
"""已改用合并+vLLM，转发到 run_pass1_only_solver_merged.py。"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "run_pass1_only_solver_merged.py"
    subprocess.run([sys.executable, str(target), *sys.argv[1:]], check=True)
