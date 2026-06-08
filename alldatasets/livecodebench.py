"""
LiveCodeBench（test5.jsonl / test6.jsonl）。

默认路径：~/lzh/datasets/LiveCodeBench（自动识别 test/ 子目录）
"""
from __future__ import annotations

import base64
import json
import pickle
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

_DEFAULT_FILES = ("test5.jsonl", "test6.jsonl")


def _decode_private_cases(data: str) -> List[Dict[str, Any]]:
    if not data:
        return []
    try:
        decoded = base64.b64decode(data)
        decompressed = zlib.decompress(decoded)
        try:
            obj = pickle.loads(decompressed)
        except Exception:
            obj = decompressed.decode("utf-8")
        if isinstance(obj, str):
            obj = json.loads(obj)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
    except Exception:
        pass
    return []


def _parse_test_cases(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception:
            return []
    return []


def _stdin_cases(cases: List[Dict[str, Any]]) -> tuple[List[str], List[str]]:
    inputs: List[str] = []
    outputs: List[str] = []
    for case in cases:
        testtype = str(case.get("testtype") or "stdin").lower()
        if testtype not in ("stdin", ""):
            continue
        inp = case.get("input", "")
        out = case.get("output", "")
        inputs.append(str(inp))
        outputs.append(str(out))
    return inputs, outputs


class LiveCodeBench:
    """与 APPS 相同接口：get / get_by_tag / get_io_inputs / get_io_outputs / foreach。"""

    def __init__(
        self,
        path: str = "",
        *,
        files: Optional[List[str]] = None,
        include_public: bool = True,
        include_private: bool = True,
    ):
        if not (path or "").strip():
            from alldatasets.loader import default_dataset_path

            path = default_dataset_path("livecodebench")
        root = Path(path).expanduser()
        if not root.is_dir():
            raise FileNotFoundError(f"LiveCodeBench 目录不存在: {root}")

        from alldatasets.loader import resolve_test_subdir

        self.path = resolve_test_subdir(root, markers=_DEFAULT_FILES)
        if not self.path.is_dir():
            raise FileNotFoundError(f"LiveCodeBench 数据目录不存在: {self.path}")

        self.include_public = include_public
        self.include_private = include_private
        self.files = list(files or _DEFAULT_FILES)

        rows: List[Dict[str, Any]] = []
        for file_name in self.files:
            file_path = self.path / file_name
            if not file_path.is_file():
                continue
            with file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except Exception:
                        continue
                    items = item if isinstance(item, list) else [item]
                    for obj in items:
                        if not isinstance(obj, dict):
                            continue
                        rows.append(
                            {
                                "idx": len(rows),
                                "id": str(obj.get("question_id") or len(rows)),
                                "description": str(obj.get("question_content") or "").strip(),
                                "question_title": str(obj.get("question_title") or ""),
                                "platform": str(obj.get("platform") or ""),
                                "difficulty": str(obj.get("difficulty") or ""),
                                "starter_code": str(obj.get("starter_code") or ""),
                                "raw": obj,
                            }
                        )

        if not rows:
            raise FileNotFoundError(
                f"未在 {self.path} 找到有效 LiveCodeBench 数据（需 {_DEFAULT_FILES}）"
            )

        self.df = pd.DataFrame(rows)
        self.df.set_index("idx", inplace=True, drop=False)

    def get(self, idx):
        try:
            return self.df.loc[idx]
        except KeyError:
            raise IndexError(f"idx={idx} 不存在")

    def _raw(self, idx: int) -> Dict[str, Any]:
        row = self.get(idx)
        raw = row.get("raw")
        return raw if isinstance(raw, dict) else {}

    def _all_cases(self, idx: int) -> List[Dict[str, Any]]:
        raw = self._raw(idx)
        cases: List[Dict[str, Any]] = []
        if self.include_public:
            cases.extend(_parse_test_cases(raw.get("public_test_cases")))
        if self.include_private:
            private = raw.get("private_test_cases")
            if isinstance(private, str):
                cases.extend(_decode_private_cases(private))
            else:
                cases.extend(_parse_test_cases(private))
        return cases

    def get_by_tag(self, tag, idx):
        row = self.get(idx)
        if tag in self.df.columns and tag != "raw":
            return row[tag]
        if tag == "description":
            return row["description"]
        if tag == "question_content":
            return row["description"]
        if tag == "question":
            return row["description"]
        if tag in ("question_title", "platform", "difficulty", "starter_code", "id"):
            return row[tag]
        if tag == "public_test_cases":
            return _parse_test_cases(self._raw(idx).get("public_test_cases"))
        if tag == "private_test_cases":
            private = self._raw(idx).get("private_test_cases")
            if isinstance(private, str):
                return _decode_private_cases(private)
            return _parse_test_cases(private)
        raise KeyError(f"未知 tag: {tag}")

    def get_io_inputs(self, idx, max_count: int = 0) -> List[str]:
        inputs, _ = _stdin_cases(self._all_cases(idx))
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_io_outputs(self, idx, max_count: int = 0) -> List[str]:
        _, outputs = _stdin_cases(self._all_cases(idx))
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)
        for idx in tqdm(range(start, end), desc="LiveCodeBench", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
