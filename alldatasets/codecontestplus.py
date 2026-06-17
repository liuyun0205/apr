from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from alldatasets.loader import datasets_root


def _default_ccp_path() -> str:
    return str(datasets_root() / "codecontestplus")


class CodeContestPlus:
    """CodeContests+ parquet：description + correct/incorrect_submissions 等。"""

    def __init__(self, path: str = ""):
        self.path = Path(path or _default_ccp_path()).expanduser()

        if self.path.is_file():
            self.df = pd.read_parquet(self.path)
        else:
            files = sorted(self.path.glob("part-*.parquet"))

            if not files:
                raise FileNotFoundError(f"没有找到 parquet 文件: {self.path}")

            self.df = pd.concat(
                [pd.read_parquet(f) for f in files],
                ignore_index=True
            )

        if "idx" not in self.df.columns:
            self.df.insert(0, "idx", range(len(self.df)))

        if "id" not in self.df.columns and "task_id" in self.df.columns:
            self.df["id"] = self.df["task_id"].astype(str)
        elif "id" not in self.df.columns:
            self.df["id"] = self.df["idx"].astype(str)

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

        aliases = {
            "question": "description",
            "problem_id": "id",
        }
        if tag in aliases and aliases[tag] in self.df.columns:
            return row[aliases[tag]]

        raise KeyError(f"未知 tag: {tag}")

    @staticmethod
    def _is_null(val: Any) -> bool:
        if val is None:
            return True
        try:
            if isinstance(val, float) and math.isnan(val):
                return True
            if pd.isna(val):
                return True
        except (TypeError, ValueError):
            pass
        return False

    @staticmethod
    def _submission_item_to_dict(item: Any) -> Optional[Dict[str, str]]:
        if CodeContestPlus._is_null(item):
            return None

        raw: Any = item
        if isinstance(raw, dict):
            data = raw
        elif hasattr(raw, "as_py"):
            data = raw.as_py()
            if not isinstance(data, dict):
                return None
        elif hasattr(raw, "_asdict"):
            data = raw._asdict()
        elif hasattr(raw, "keys"):
            try:
                data = {k: raw[k] for k in raw.keys()}
            except (TypeError, KeyError):
                return None
        else:
            return None

        code = data.get("code", data.get("solution", data.get("program", "")))
        language = data.get("language", data.get("lang", ""))
        if isinstance(code, bytes):
            code = code.decode("utf-8", errors="replace")
        if isinstance(language, bytes):
            language = language.decode("utf-8", errors="replace")
        code = str(code or "").strip()
        if not code:
            return None
        return {"code": code, "language": str(language or "")}

    @staticmethod
    def _coerce_submission_list(val: Any) -> List[Any]:
        if CodeContestPlus._is_null(val):
            return []
        if isinstance(val, str):
            text = val.strip()
            if not text:
                return []
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return []
            val = parsed
        if hasattr(val, "tolist"):
            val = val.tolist()
        if isinstance(val, dict):
            return [val]
        if isinstance(val, (list, tuple)):
            return list(val)
        return []

    @classmethod
    def _normalize_submissions(cls, val: Any) -> List[dict]:
        out: List[dict] = []
        for item in cls._coerce_submission_list(val):
            rec = cls._submission_item_to_dict(item)
            if rec:
                out.append(rec)
        return out

    @staticmethod
    def _looks_like_python_code(code: str) -> bool:
        text = (code or "").lstrip()
        if not text:
            return False
        non_py_markers = (
            "#include",
            "using namespace std",
            "import java.",
            "public class ",
            "package main",
            "fn main(",
            "using System;",
        )
        if any(m in text for m in non_py_markers):
            return False
        py_markers = (
            "def ",
            "import ",
            "print(",
            "input(",
            "sys.stdin",
            "if __name__",
        )
        return any(m in text for m in py_markers)

    @classmethod
    def _matches_language(cls, language: str, code: str, filter_lang: str) -> bool:
        if not filter_lang:
            return True
        lang = (language or "").strip().lower()
        filt = filter_lang.strip().lower()
        if filt == "python":
            if any(x in lang for x in ("python", "pypy", "py3")):
                return True
            if not lang:
                return cls._looks_like_python_code(code)
            return False
        return filt in lang

    def get_correct_submissions(self, idx: int) -> List[dict]:
        """返回 [{code, language}, ...]。"""
        row = self.get(idx)
        raw = row["correct_submissions"] if "correct_submissions" in self.df.columns else None
        return self._normalize_submissions(raw)

    def get_accepted_solutions(
        self,
        idx: int,
        *,
        language: Optional[str] = "python",
    ) -> List[dict]:
        """过滤语言后的 correct_submissions（SFT 冷启动用）。"""
        subs = self.get_correct_submissions(idx)
        if not language:
            return subs
        return [
            s
            for s in subs
            if self._matches_language(
                str(s.get("language") or ""),
                str(s.get("code") or ""),
                language,
            )
        ]

    @staticmethod
    def _testcase_item_to_pair(item: Any) -> Optional[tuple[str, str]]:
        if CodeContestPlus._is_null(item):
            return None
        raw: Any = item
        if isinstance(raw, dict):
            data = raw
        elif hasattr(raw, "as_py"):
            data = raw.as_py()
            if not isinstance(data, dict):
                return None
        elif hasattr(raw, "keys"):
            try:
                data = {k: raw[k] for k in raw.keys()}
            except (TypeError, KeyError):
                return None
        else:
            return None

        inp = data.get("input", data.get("stdin", ""))
        out = data.get("output", data.get("stdout", data.get("expected_output", "")))
        if isinstance(inp, bytes):
            inp = inp.decode("utf-8", errors="replace")
        if isinstance(out, bytes):
            out = out.decode("utf-8", errors="replace")
        inp = str(inp or "")
        out = str(out or "")
        if not inp and not out:
            return None
        return inp, out

    @classmethod
    def _normalize_test_cases(cls, val: Any) -> List[tuple[str, str]]:
        pairs: List[tuple[str, str]] = []
        items: List[Any]
        if cls._is_null(val):
            return pairs
        if isinstance(val, str):
            text = val.strip()
            if not text:
                return pairs
            try:
                val = json.loads(text)
            except json.JSONDecodeError:
                return pairs
        if hasattr(val, "tolist"):
            val = val.tolist()
        if isinstance(val, dict):
            items = [val]
        elif isinstance(val, (list, tuple)):
            items = list(val)
        else:
            return pairs
        for item in items:
            pair = cls._testcase_item_to_pair(item)
            if pair is not None:
                pairs.append(pair)
        return pairs

    def get_test_cases(self, idx: int) -> List[tuple[str, str]]:
        """返回 [(input, output), ...]；default 配置无 test_cases 时为空。"""
        if "test_cases" not in self.df.columns:
            return []
        row = self.get(idx)
        return self._normalize_test_cases(row["test_cases"])

    def get_io_inputs(self, idx: int, max_count: int = 0) -> List[str]:
        pairs = self.get_test_cases(idx)
        inputs = [p[0] for p in pairs]
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_io_outputs(self, idx: int, max_count: int = 0) -> List[str]:
        pairs = self.get_test_cases(idx)
        outputs = [p[1] for p in pairs]
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def get_public_io_inputs(self, idx: int, max_count: int = 0) -> List[str]:
        return self.get_io_inputs(idx, max_count=max_count)

    def get_public_io_outputs(self, idx: int, max_count: int = 0) -> List[str]:
        return self.get_io_outputs(idx, max_count=max_count)

    def diagnose_submissions(
        self,
        *,
        start: int = 0,
        end: Optional[int] = None,
        sample_size: int = 3,
    ) -> Dict[str, Any]:
        """调试 correct_submissions 解析情况。"""
        end = len(self.df) if end is None else min(end, len(self.df))
        stats = {
            "columns": list(self.df.columns),
            "has_correct_submissions": "correct_submissions" in self.df.columns,
            "rows": end - start,
            "raw_nonempty": 0,
            "parsed_nonempty": 0,
            "python_nonempty": 0,
            "language_samples": [],
            "raw_type_samples": [],
            "parsed_count_samples": [],
        }
        for idx in range(start, end):
            row = self.get(idx)
            raw = row["correct_submissions"] if stats["has_correct_submissions"] else None
            if not self._is_null(raw) and str(raw).strip():
                stats["raw_nonempty"] += 1
            parsed = self.get_correct_submissions(idx)
            if parsed:
                stats["parsed_nonempty"] += 1
            py = self.get_accepted_solutions(idx, language="python")
            if py:
                stats["python_nonempty"] += 1
            if len(stats["raw_type_samples"]) < sample_size:
                stats["raw_type_samples"].append(type(raw).__name__)
                stats["parsed_count_samples"].append(len(parsed))
            for s in parsed[:2]:
                lang = str(s.get("language") or "")
                if lang and lang not in stats["language_samples"]:
                    stats["language_samples"].append(lang)
                if len(stats["language_samples"]) >= 20:
                    break
        return stats

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)

        for idx in tqdm(range(start, end), desc="CodeContestPlus", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
