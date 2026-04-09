import json
import os
from collections import Counter
from model import model
from config import get_args
from utils import write2file,run_program,get_filename
class dataset():
    def __init__(self,root):
        self.root = root
        self.data={}
        self.id_map={}
        self.details_map={}
    def load(self, language):
        filepath = os.path.join(self.root, f"{language}.jsonl")
        problem_descriptions=os.path.join("xcodeeval", f"problem_descriptions.jsonl")
        self.data[language] = []
        with open(problem_descriptions, "r", encoding="utf-8") as f:
            for line in f:
                d = json.loads(line)
                key = d["src_uid"]

                self.details_map[key] = d
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                self.data[language].append(item)
                uid = item["bug_code_uid"]
                self.id_map[uid] = item

    def read(self, language, line_id, field=None):
        if language not in self.data:
            self.load(language)

        if line_id < 0 or line_id >= len(self.data[language]):
            return None

        item = self.data[language][line_id]
        if field is None:
            return item
        if field in ["description"]:
            src_uid = item.get("src_uid")
            details = self.details_map.get(src_uid)
            if details is None:
                return None
            return details.get(field)
        return item.get(field)
    def eval(self,model,languages=["Rust"]):
        testfilepath="xcodeeval/unittest_db.json"
        with open(testfilepath, "r", encoding="utf-8") as f:
            testcases = json.load(f)

        for language in languages:

            passcount = 0
            total = 0
            failcount = 0
            filepath = os.path.join(self.root, f"{language}.jsonl")
            with open(filepath, "r", encoding="utf-8") as f:
                for line_id,line in enumerate(f):
                    item = json.loads(line)
                    total += 1
                    code=item.get("bug_source_code")
                    description = self.read(language,line_id,"description")
                    question = f"""
Task:
{description}

Buggy program:
{code}
"""
                    revise=model.main(question)
                    write2file(get_filename(language),revise)
                    uid = self.read(language,line_id,"src_uid")
                    cases = testcases.get(uid, [])
                    all_pass=True
                    for case in cases:
                        input_data = case["input"]
                        expected_output = case["output"][0].strip()

                        stdout, stderr =run_program(language,get_filename(language), input_data)

                        print("input:", repr(input_data))
                        print("expected:", expected_output)
                        print("stdout:", repr(stdout))
                        print("stderr:", repr(stderr))

                        if stderr.strip() != "" or stdout.strip() != expected_output:
                            all_pass = False
                            break

                    if all_pass:
                        print("PASS")
                        passcount += 1
                    else:
                        failcount += 1
                print(f"\n=== {language} ===")
                print(f"Total: {total}")
                print(f"Pass: {passcount}")
                print(f"Fail: {failcount}")
                if total > 0:
                    print(f"Pass Rate: {passcount / total:.2%}")
if __name__ == "__main__":
    dataset = dataset("xcodeeval/sub_test")
    language=["kotlin"]
    args = get_args()

    model = model(args)
    dataset.eval(model,language)

