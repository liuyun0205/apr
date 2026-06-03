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

    def __init__(self, ds):

        self.dataset = ds

        self.input_trigger = Agent(
            model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
            system_prompt=utils.file2text(
                "prompt/input_trriger.txt"
            ),
            device="cuda:1"
        )

        self.naivesolver = Agent(
            model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
            system_prompt=utils.file2text(
                "prompt/naivesolver.txt"
            ),
            device="cuda:2"
        )

        self.solver = Agent(
            model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
            system_prompt=utils.file2text(
                "prompt/solver.txt"
            ),
            device="cuda:3"
        )
    def generate_candidates(self,naive_bestofn,solver_bestofn,question):
        naive_codes = []
        solver_codes = []
        outs=self.generate_input(question,10)
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
    def generate_input(self,question,count=10):
        code=self.input_trigger.chat(question)
        code=utils.clean_code(code)
        out,err= utils.run_code(code)
        return out
if __name__=='__main__':
    dataset=CodeContestPlus()
    print("数据集加载完成!")
    model=Model(dataset)



