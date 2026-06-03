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
import utils
from LLM import LLM, LLMConfig
from config import get_args
from alldatasets.codecontestplus import CodeContestPlus

class model():
    def __init__(self,ds):
        self.input_trriger=LLM(LLMConfig(
            model_type="api",  # 或 "direct"
            model="input_trriger",  # 需与 serve 时模型名一致
            base_url="http://127.0.0.1:8000/v1/chat/completions",
            system_prompt=utils.file2text(f"prompt/input_trriger.txt"),
            model_path=""
        ))
        self.naivesolver = LLM(LLMConfig(
            model_type="api",  # 或 "direct"
            model="naivesolver",  # 需与 serve 时模型名一致
            base_url="http://127.0.0.1:8000/v1/chat/completions",
            system_prompt=utils.file2text(f"prompt/naivesolver.txt"),
            model_path=""
        ))
        self.solver = LLM(LLMConfig(
            model_type="api",  # 或 "direct"
            model="solver",  # 需与 serve 时模型名一致
            base_url="http://127.0.0.1:8001/v1/chat/completions",
            system_prompt=utils.file2text(f"prompt/solver.txt"),
            model_path=""
        ))
        self.dataset=ds

    def input_generate(self,input_count=10,json_file="input_trigger_result.jsonl"):
        self.dataset.foreach(self.input_trriger_single)

    def input_trriger_single(self,idx,question,json_file="input_trigger_result.jsonl"):
        res = self.input_trriger.chat(question)

        problem_id = self.dataset.get_by_tag("id",idx)
        record = {
            "idx": int(idx),
            "id": str(problem_id),
            "code": res
        }
        with open(json_file,"a",encoding="utf-8") as f:
            f.write(json.dumps(record,ensure_ascii=False)+ "\n")

        return res

    def generate_candidates(self,naivesolverBestofN,solverBestofN,question):
        naive_codes=[]
        solve_codes=[]
        for _ in range(naivesolverBestofN):
            naive_codes.append(
                self.naivesolver.chat(question)
            )
        for _ in range(solverBestofN):
            solve_codes.append(
                self.solver.chat(question)
            )
        return {
            "naive_codes": naive_codes,
            "solver_codes": solve_codes
        }



if __name__=='__main__':


    df=CodeContestPlus("~/lzh/datasets/codecontestplus")
    model = model(df)
    model.input_generate()

