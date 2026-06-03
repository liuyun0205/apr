import argparse
import csv
import json
import os
import sys
from pathlib import Path
import pandas as pd
import torch
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)
from agent import Agent
import utils
from LLM import LLM, LLMConfig
from config import get_args
from alldatasets.codecontestplus import CodeContestPlus

class Model:

    def __init__(
        self,
        ds,
        model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
        devices=None,
        lr=1e-5,
        use_lora: bool = False,
        lora_r: int = 8,
        lora_alpha: int = 16,
        lora_dropout: float = 0.05,
        gradient_checkpointing: bool = False,
        exec_kwargs=None,
    ): 
        self._exec_kwargs = exec_kwargs or {}
        self.dataset = ds
        if devices is None:
            devices = ("cuda:0", "cuda:1", "cuda:2")
        trigger_dev, naive_dev, solver_dev = devices

        lora_kwargs = dict(
            lora_r=lora_r,
            lora_alpha=lora_alpha,
            lora_dropout=lora_dropout,
            gradient_checkpointing=gradient_checkpointing,
        )

        self.input_trigger = Agent(
            model_path=model_path,
            system_prompt=utils.file2text(
                "prompt/input_trriger.txt"
            ),
            device=trigger_dev,
            trainable=False,
            use_lora=False,
        )

        self.naivesolver = Agent(
            model_path=model_path,
            system_prompt=utils.file2text(
                "prompt/naivesolver.txt"
            ),
            device=naive_dev,
            trainable=False,
            use_lora=False,
        )

        self.solver = Agent(
            model_path=model_path,
            system_prompt=utils.file2text(
                "prompt/solver.txt"
            ),
            device=solver_dev,
            lr=lr,
            trainable=True,
            use_lora=use_lora,
            **lora_kwargs,
        )
    def generate_candidates(
        self,
        naive_bestofn,
        solver_bestofn,
        question,
        input_count=10,
    ):
        naive_codes = []
        solver_codes = []
        outs = self.generate_input(question, count=input_count)
        for _ in range(naive_bestofn):
            naive_codes.append(
                self.naivesolver.chat(question)
            )

        for _ in range(solver_bestofn):
            solver_codes.append(
                self.solver.chat(question)
            )

        return {
            "naive_codes": naive_codes,
            "solver_codes": solver_codes,
            "inputs":outs
        }
    def generate_input(self, question, count=10):
        code = self.input_trigger.chat(question)
        code = utils.clean_code(code)
        stdout, _stderr = utils.run_code(code, **self._exec_kwargs)
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        return lines[:count]
if __name__=='__main__':
    dataset=CodeContestPlus()
    print("数据集加载完成!")
    model=Model(dataset)



