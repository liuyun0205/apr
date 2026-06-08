"""
CodeForces 评测集（Gen-Verse / CURE）。

默认路径：~/lzh/datasets/codeforces（优先 test/ 子目录）

支持：
  - test/test-00000-of-00001.parquet（HF 分片）
  - test/part-*.parquet
  - test/CodeForces.json / CodeForces.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from tqdm import tqdm

_PARQUET_GLOBS = ("test-*.parquet", "part-*.parquet", "*.parquet")


def _load_json_records(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    data = json.loads(text)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("data", "problems", "items", "test"):
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        if "question" in data:
            return [data]
    return []


def _search_dirs(root: Path) -> List[Path]:
    if (root / "test").is_dir():
        return [root / "test", root]
    return [root]


def _collect_parquet_files(root: Path) -> List[Path]:
    found: List[Path] = []
    for base in _search_dirs(root):
        for pattern in _PARQUET_GLOBS:
            found.extend(sorted(base.glob(pattern)))
    # 去重保序
    seen = set()
    out: List[Path] = []
    for p in found:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _resolve_json_path(root: Path) -> Optional[Path]:
    if root.is_file() and root.suffix.lower() == ".json":
        return root

    names = ("CodeForces.json", "codeforces.json", "Codeforces.json")
    for base in _search_dirs(root):
        for name in names:
            cand = base / name
            if cand.is_file():
                return cand
        for c in sorted(base.glob("*.json")):
            if _load_json_records(c):
                return c
    return None


def _resolve_data_source(root: Path) -> tuple[str, Any]:
    if root.is_file():
        if root.suffix.lower() == ".parquet":
            return "parquet", [root]
        if root.suffix.lower() == ".json":
            return "json", root

    parquet_files = _collect_parquet_files(root)
    if parquet_files:
        return "parquet", parquet_files

    json_path = _resolve_json_path(root)
    if json_path is not None:
        return "json", json_path

    raise FileNotFoundError(
        f"CodeForces 路径无效: {root}\n"
        "需要 test/test-*.parquet、part-*.parquet 或 test/CodeForces.json"
    )


def _as_str_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x) for x in raw]
    if hasattr(raw, "tolist"):
        try:
            val = raw.tolist()
            if isinstance(val, list):
                return [str(x) for x in val]
        except Exception:
            pass
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except Exception:
            return [raw]
    return []


def _case_dict_list(raw: Any) -> List[Dict[str, Any]]:
    """解析 [{input, output}, ...] 结构（parquet 的 examples / official_tests）。"""
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [raw] if ("input" in raw or "output" in raw) else []
    if isinstance(raw, (list, tuple)):
        return [x for x in raw if isinstance(x, dict)]
    if hasattr(raw, "tolist"):
        try:
            val = raw.tolist()
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
            if isinstance(val, dict):
                return [val]
        except Exception:
            pass
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            return _case_dict_list(json.loads(text))
        except Exception:
            return []
    return []


def _io_from_case_dicts(cases: List[Dict[str, Any]]) -> tuple[List[str], List[str]]:
    inputs: List[str] = []
    outputs: List[str] = []
    for case in cases:
        inp = case.get("input")
        out = case.get("output")
        if inp is None and out is None:
            continue
        inputs.append("" if inp is None else str(inp))
        outputs.append("" if out is None else str(out))
    return inputs, outputs


def _row_question(obj: Dict[str, Any]) -> str:
    return str(
        obj.get("question")
        or obj.get("question_content")
        or obj.get("description")
        or ""
    ).strip()


def _row_id(obj: Dict[str, Any], fallback: int) -> str:
    for key in ("task_id", "id", "question_id", "problem_id"):
        if key in obj and str(obj[key]).strip() != "":
            return str(obj[key])
    return str(fallback)


class CodeForces:
    """与 APPS 相同接口：get / get_by_tag / get_io_inputs / get_io_outputs / foreach。"""

    def __init__(
        self,
        path: str = "",
        *,
        use_public: bool = True,
        use_private: bool = True,
    ):
        if not (path or "").strip():
            from alldatasets.loader import default_dataset_path

            path = default_dataset_path("codeforces")

        self.root = Path(path).expanduser()
        self.use_public = use_public
        self.use_private = use_private

        mode, source = _resolve_data_source(self.root)
        self._mode = mode

        if mode == "parquet":
            files: Sequence[Path] = source  # type: ignore[assignment]
            self.data_path = Path(files[0]).parent if files else self.root
            self.df = pd.concat(
                [pd.read_parquet(str(f)) for f in files],
                ignore_index=True,
            )
            if "description" not in self.df.columns and "question" in self.df.columns:
                self.df["description"] = self.df["question"].astype(str)
            if "id" not in self.df.columns and "task_id" in self.df.columns:
                self.df["id"] = self.df["task_id"].astype(str)
            if "idx" not in self.df.columns:
                self.df.insert(0, "idx", range(len(self.df)))
            self.df.set_index("idx", inplace=True, drop=False)
            self._json_records = None
        else:
            json_path: Path = source  # type: ignore[assignment]
            self.data_path = json_path.parent
            records = _load_json_records(json_path)
            if not records:
                raise FileNotFoundError(f"JSON 无有效题目: {json_path}")
            rows: List[Dict[str, Any]] = []
            for i, obj in enumerate(records):
                rows.append(
                    {
                        "idx": i,
                        "id": _row_id(obj, i),
                        "description": _row_question(obj),
                        "difficulty": str(obj.get("difficulty") or ""),
                        "raw": obj,
                    }
                )
            self.df = pd.DataFrame(rows)
            self.df.set_index("idx", inplace=True, drop=False)
            self._json_records = records

        self.path = self.data_path

    def get(self, idx):
        try:
            return self.df.loc[idx]
        except KeyError:
            raise IndexError(f"idx={idx} 不存在")

    def _raw(self, idx: int) -> Dict[str, Any]:
        if self._mode == "json":
            raw = self.get(idx).get("raw")
            return raw if isinstance(raw, dict) else {}

        row = self.get(idx)
        return {col: row[col] for col in self.df.columns if col not in ("idx",)}

    def _io_lists(self, idx: int) -> tuple[List[str], List[str]]:
        raw = self._raw(idx)
        inputs: List[str] = []
        outputs: List[str] = []

        if self.use_public:
            pub_in = _as_str_list(raw.get("example_input"))
            pub_out = _as_str_list(raw.get("example_output"))
            if pub_in:
                inputs.extend(pub_in)
                outputs.extend(pub_out)
            else:
                pi, po = _io_from_case_dicts(_case_dict_list(raw.get("examples")))
                inputs.extend(pi)
                outputs.extend(po)

        if self.use_private:
            priv_in = _as_str_list(raw.get("test_input"))
            priv_out = _as_str_list(raw.get("test_output"))
            if priv_in:
                inputs.extend(priv_in)
                outputs.extend(priv_out)
            else:
                pi, po = _io_from_case_dicts(_case_dict_list(raw.get("official_tests")))
                inputs.extend(pi)
                outputs.extend(po)

        if not inputs:
            inputs = _as_str_list(raw.get("inputs"))
            outputs = _as_str_list(raw.get("outputs"))

        n = min(len(inputs), len(outputs))
        return inputs[:n], outputs[:n]

    def get_by_tag(self, tag, idx):
        row = self.get(idx)

        if tag in self.df.columns and tag not in ("raw",):
            return row[tag]

        if tag in ("description", "question", "question_content"):
            if "description" in self.df.columns and str(row.get("description", "")).strip():
                return str(row["description"])
            return _row_question(self._raw(idx))

        if tag == "id":
            if "id" in self.df.columns:
                return str(row["id"])
            return _row_id(self._raw(idx), int(idx))

        if tag == "test_time_limit":
            raw = self._raw(idx)
            return int(raw.get("test_time_limit") or 10)

        raise KeyError(f"未知 tag: {tag}")

    def get_io_inputs(self, idx, max_count: int = 0) -> List[str]:
        inputs, _ = self._io_lists(idx)
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_io_outputs(self, idx, max_count: int = 0) -> List[str]:
        _, outputs = self._io_lists(idx)
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)
        for idx in tqdm(range(start, end), desc="CodeForces", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
