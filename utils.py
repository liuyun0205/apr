import os
import shutil
import subprocess
from collections import Counter

import subprocess

def run_program(language, filename, input_data):
    language = language.lower()

    try:
        if language == "python":
            try:
                run_res = subprocess.run(
                    ["python", filename],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "cpp" or language == "c++":
            compile_res = subprocess.run(
                ["g++", filename, "-o", "a.exe"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    ["a.exe"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "rust":
            compile_res = subprocess.run(
                ["rustc", filename, "-o", "main.exe"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    ["main.exe"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        elif language == "kotlin":
            kotlinc_path = r"E:\kotlinc\bin\kotlinc.BAT"
            java_path = r"C:\Program Files (x86)\Common Files\Oracle\Java\java8path\java.EXE"

            compile_res = subprocess.run(
                [kotlinc_path, filename, "-include-runtime", "-d", "main.jar"],
                capture_output=True,
                text=True
            )
            if compile_res.returncode != 0:
                return "", compile_res.stderr

            try:
                run_res = subprocess.run(
                    [java_path, "-jar", "main.jar"],
                    input=input_data,
                    capture_output=True,
                    text=True,
                    timeout=3
                )
                return run_res.stdout.strip(), run_res.stderr.strip()
            except subprocess.TimeoutExpired:
                return "", "TIME_LIMIT_EXCEEDED"

        else:
            return "", f"Unsupported language: {language}"

    except Exception as e:
        return "", str(e)
def get_filename(language):
    if language == "python":
        return "test.py"
    elif language == "cpp":
        return "test.cpp"
    elif language=="C++":
        return "test.cpp"
    elif language == "java":
        return "Main.java"
    elif language == "js":
        return "test.js"
    elif language == "go":
        return "intersperse_test.go"
    elif language == "rust" or language=="Rust":
        return "testbox/rust/src/main.rs"
    elif language == "kotlin":
        return "testbox/kotlin/main.kt"
    else:
        raise ValueError(f"Unsupported language: {language}")
def write2file(filename, content):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
def file2text(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()
    return text
def clean_check_file(dataset, language, input_file="data.txt"):
    if language not in dataset.data:
        dataset.load(language)
    data_uid_set = set(
        item.get("src_uid")
        for item in dataset.data[language]
        if item.get("src_uid")
    )
    with open(input_file, "r", encoding="utf-8") as f:
        file_uids = [line.strip() for line in f if line.strip()]
    filtered_uids = [uid for uid in file_uids if uid in data_uid_set]

    with open(input_file, "w", encoding="utf-8") as f:
        for uid in filtered_uids:
            f.write(uid + "\n")

    print(f"原始数量: {len(file_uids)}")
    print(f"保留数量: {len(filtered_uids)}")
    print(f"删除数量: {len(file_uids) - len(filtered_uids)}")
import json

def extract_by_src_uid(dataset, language, input_file="data.txt", output_file="C#.jsonl"):
    # 读取 uid
    with open(input_file, "r", encoding="utf-8") as f:
        uid_set = set(line.strip() for line in f if line.strip())

    # 加载数据
    if language not in dataset.data:
        dataset.load(language)

    count = 0
    with open(output_file, "w", encoding="utf-8") as out:
        for item in dataset.data[language]:
            src_uid = item.get("src_uid")

            if src_uid not in uid_set:
                continue

            # ✅ 只保留主数据
            record = {
                "src_uid": src_uid,
                "bug_code_uid": item.get("bug_code_uid"),
                "apr_id": item.get("apr_id"),
                "lang": item.get("lang"),
                "lang_cluster": item.get("lang_cluster"),
                "difficulty": item.get("difficulty"),
                "tags": item.get("tags"),
                "bug_exec_outcome": item.get("bug_exec_outcome"),
                "potential_dominant_fix_op": item.get("potential_dominant_fix_op"),
                "similarity_score": item.get("similarity_score"),
                "bug_source_code": item.get("bug_source_code")
            }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    print(f"写入完成，共 {count} 条 → {output_file}")
def count_outcome(dataset, language):
    outcomes = []

    for item in dataset.data[language]:
        outcomes.append(item.get("bug_exec_outcome"))

    counter = Counter(outcomes)
    return counter
