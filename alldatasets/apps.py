"""
APPS 数据集（目录结构同 get_codeforces_data/APPS/train）。

每题一个子目录，例如：
  train/0000/question.txt
  train/0000/solution.py
  train/0000/input_output.json
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
from tqdm import tqdm

# 与 get_codeforces_data/utils.py 一致：截到 sample input/output 等段落之前
_SECTION_PREFIXES = (
    "input",
    "output",
    "sample input",
    "sample output",
    "examples",
    "example",
)


def _read_text(path: Path) -> str:
    encodings = ("utf-8", "utf-8-sig", "gbk", "latin-1")
    last_err = None
    for enc in encodings:
        try:
            return path.read_text(encoding=enc).strip()
        except UnicodeDecodeError as e:
            last_err = e
    raise last_err or RuntimeError(f"无法读取: {path}")


def extract_pure_problem(text: str) -> str:
    """去掉题面末尾的 Input/Output/Examples 样例块，只保留题目描述。"""
    lines = text.splitlines()
    pure_lines = []

    for line in lines:
        stripped = line.strip().lower()
        if stripped.startswith("#"):
            stripped_no_hash = stripped.lstrip("#").strip()
        else:
            stripped_no_hash = stripped

        hit_section = any(
            stripped_no_hash.startswith(prefix) for prefix in _SECTION_PREFIXES
        )
        if hit_section:
            break
        pure_lines.append(line)

    while pure_lines and pure_lines[-1].strip() == "":
        pure_lines.pop()

    return "\n".join(pure_lines).strip("\n")


def _loads_input_output_json(text: str) -> Optional[Any]:
    """
    解析 input_output.json。
    APPS 中偶发超大整数测例，需放宽 Python 3.11+ 的 int 字符串长度限制。
    """
    text = (text or "").strip()
    if not text:
        return None

    old_limit = None
    setter = getattr(sys, "set_int_max_str_digits", None)
    getter = getattr(sys, "get_int_max_str_digits", None)
    if callable(setter) and callable(getter):
        old_limit = getter()
        setter(0)

    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    finally:
        if old_limit is not None and callable(setter):
            setter(old_limit)


def parse_input_output_inputs(text: str) -> List[str]:
    """解析 APPS input_output.json，返回 inputs 列表（每项为一条测例 stdin）。"""
    data = _loads_input_output_json(text)
    if not isinstance(data, dict):
        return []

    raw = data.get("inputs")
    if not isinstance(raw, list):
        return []

    out: List[str] = []
    for item in raw:
        s = str(item)
        if s.strip():
            out.append(s)
    return out


def parse_input_output_outputs(text: str) -> List[str]:
    """解析 APPS input_output.json，返回 outputs 列表（每项为一条测例期望 stdout）。"""
    data = _loads_input_output_json(text)
    if not isinstance(data, dict):
        return []

    raw = data.get("outputs")
    if not isinstance(raw, list):
        return []

    out: List[str] = []
    for item in raw:
        s = str(item)
        if s.strip():
            out.append(s)
    return out


class APPS:
    """与 CodeContestPlus 相同接口：get / get_by_tag / foreach。"""

    TAG_FILES = {
        "question": "question.txt",
        "solution": "solution.py",
        "input_output": "input_output.json",
        "metadata": "metadata.json",
    }

    def __init__(
        self,
        path: str = "~/get_codeforces_data/APPS/train",
        *,
        strip_samples: bool = True,
        require_question: bool = True,
    ):
        self.path = Path(path).expanduser()
        if not self.path.is_dir():
            raise FileNotFoundError(f"APPS 目录不存在: {self.path}")

        rows = []
        prob_re = re.compile(r"^\d{4}$")
        for prob_dir in sorted(self.path.iterdir()):
            if not prob_dir.is_dir() or not prob_re.match(prob_dir.name):
                continue
            qfile = prob_dir / "question.txt"
            if require_question and not qfile.exists():
                continue

            question_raw = _read_text(qfile) if qfile.exists() else ""
            description = (
                extract_pure_problem(question_raw)
                if strip_samples
                else question_raw
            )
            rows.append(
                {
                    "idx": len(rows),
                    "id": prob_dir.name,
                    "description": description,
                    "question_raw": question_raw,
                    "dir": str(prob_dir),
                }
            )

        if not rows:
            raise FileNotFoundError(
                f"未在 {self.path} 下找到有效题目（需 0000 形式子目录 + question.txt）"
            )

        self.df = pd.DataFrame(rows)
        self.df.set_index("idx", inplace=True, drop=False)

    def get(self, idx):
        try:
            return self.df.loc[idx]
        except KeyError:
            raise IndexError(f"idx={idx} 不存在")

    def get_by_tag(self, tag, idx):
        row = self.get(idx)

        if tag in self.df.columns:
            return row[tag]

        if tag == "description":
            return row["description"]

        if tag in self.TAG_FILES:
            prob_dir = Path(row["dir"])
            fpath = prob_dir / self.TAG_FILES[tag]
            if not fpath.exists():
                raise FileNotFoundError(f"缺少文件: {fpath}")
            return _read_text(fpath)

        raise KeyError(f"未知 tag: {tag}")

    def problem_dir(self, idx) -> Path:
        return Path(self.get_by_tag("dir", idx))

    def get_io_inputs(self, idx, max_count: int = 10) -> List[str]:
        """读取题目目录下 input_output.json 的 inputs 字段。"""
        prob_dir = self.problem_dir(idx)
        io_path = prob_dir / "input_output.json"
        if not io_path.exists():
            return []
        text = _read_text(io_path)
        inputs = parse_input_output_inputs(text)
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_io_outputs(self, idx, max_count: int = 10) -> List[str]:
        """读取题目目录下 input_output.json 的 outputs 字段。"""
        prob_dir = self.problem_dir(idx)
        io_path = prob_dir / "input_output.json"
        if not io_path.exists():
            return []
        text = _read_text(io_path)
        outputs = parse_input_output_outputs(text)
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)

        for idx in tqdm(range(start, end), desc="APPS", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
