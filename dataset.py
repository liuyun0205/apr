import os
from string import Template

import pandas as pd
from utils import run_program,write2file,get_filename
from model import model
from config import get_args
class dataset():
    def __init__(self,root):
        self.root=root
    def read(self, column, language, task_id):
        filepath = os.path.join(self.root, language, "test-00000-of-00001.parquet")
        df = pd.read_parquet(filepath)
        if column not in df.columns:
            raise ValueError(f"{column} not exist!")

        if task_id >= len(df):
            raise ValueError(f"{task_id} out of range!")

        return df.iloc[task_id][column]

    def combine_program(self, language, task_id):
        declaration = self.read("declaration", language, task_id)
        buggy_solution = self.read("buggy_solution", language, task_id)
        test = self.read("test", language, task_id)
        test_setup=self.read("test_setup", language, task_id)
        if language=="go":
            test=""
        elif language=="rust":
            declaration=self.read("prompt",language,task_id)
        program = test_setup+"\n"+declaration + "\n" + buggy_solution

        return program
    def eval(self,model,language=["rust"]):#,"java","python","js","cpp","go"]):

        for language in language:
            passcount=0
            total=0
            failcount=0
            filepath = os.path.join(self.root, language, "test-00000-of-00001.parquet")
            df=pd.read_parquet(filepath)
            for raw_task_id in df["task_id"]:
                total+=1
                task_id = int(raw_task_id.split("/")[-1])  # Python/0 → 0
                declaration = dataset.read("docstring", language, task_id)

                #print("---------------prompt----------:\n"+prompt+"\n----------end-------------\n")
                program = self.combine_program(language, task_id)

                question = f"""
Task:
{declaration}

Buggy program:
{program}
"""
                revise=model.main(question)
                test = self.read("test", language, task_id)
                revise=program+"\n"+test
                write2file(get_filename(language),revise)
                code, out, err=run_program(language,get_filename(language))
                if code == 0:
                    passcount += 1
                    print("PASS")
                else:
                    failcount += 1
                    save_dir = os.path.join("humanevalpack", language)
                    os.makedirs(save_dir, exist_ok=True)
                    file_path = os.path.join(save_dir, f"{task_id}.rs")

                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(revise)
                    print("FAIL")
                    print(err)
            print("-------------")
            print("PASS:", passcount)
            print("FAIL:", failcount)
            print("TOTAL:", total)
            print("PASS RATE:", passcount / total)

if __name__=="__main__":
    dataset=dataset("humanevalpack")
    args=get_args()

    model=model(args)
    dataset.eval(model)