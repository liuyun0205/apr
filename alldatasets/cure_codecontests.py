"""
CURE_codecontests（CURE 论文整理的 CodeContests JSON 格式）。

目录结构：
  CURE_codecontests/
    train/CodeContests_train.json   # 4529 题
    test/CodeContests.json          # 239 题

每题字段：
  question        题面
  task_id         题目 id
  example_input / example_output    题干样例（public）
  test_input / test_output          隐藏测例（private）

与 APPS/CodeContests 相同接口：
  get / get_by_tag / get_io_inputs / get_io_outputs /
  get_public_io_inputs / get_public_io_outputs / foreach
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from tqdm import tqdm


def _resolve_json_path(path: str, split: str) -> Path:
    """path 可为：json 文件 / split 子目录 / 含 train|test 的根目录。"""
    p = Path(path).expanduser()
    if p.is_file() and p.suffix == ".json":
        return p
    candidates: List[Path] = []
    if split:
        candidates.append(p / split)
    candidates.append(p)
    for cand in candidates:
        if cand.is_dir():
            jsons = sorted(cand.glob("*.json"))
            if jsons:
                return jsons[0]
    raise FileNotFoundError(
        f"CURE_codecontests 路径无效: {p}（split={split!r}）\n"
        "需要 json 文件、含 *.json 的目录，或含 train/test 子目录的根目录"
    )


class CURECodeContests:
    """与 APPS 相同接口：get / get_by_tag / get_io_inputs / foreach。"""

    def __init__(
        self,
        path: str = "~/datasets/CURE_codecontests",
        *,
        split: str = "train",
        rollout_io_source: str = "tests",
        public_io_source: str = "sample",
        io_source: str = "",
    ):
        self.split = (split or "train").strip().lower()
        self.json_path = _resolve_json_path(path, self.split)

        legacy = (io_source or "").strip().lower()
        self.rollout_io_source = (
            (rollout_io_source or legacy or "tests").strip().lower()
        )
        self.public_io_source = (public_io_source or "sample").strip().lower()
        for name, src in (
            ("rollout_io_source", self.rollout_io_source),
            ("public_io_source", self.public_io_source),
        ):
            if src not in ("tests", "sample"):
                raise ValueError(f"{name} 须为 tests 或 sample，收到: {src!r}")
        self.io_source = self.rollout_io_source

        with self.json_path.open(encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, list) or not raw:
            raise ValueError(f"json 内容应为非空 list: {self.json_path}")

        self._items: List[dict] = []
        rows: List[dict] = []
        for item in raw:
            question = str(item.get("question") or "").strip()
            if not question:
                continue
            idx = len(rows)
            self._items.append(item)
            rows.append(
                {
                    "idx": idx,
                    "id": str(item.get("task_id", idx)),
                    "description": question,
                }
            )

        if not rows:
            raise FileNotFoundError(f"未在 {self.json_path} 找到有效题目")

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
        item = self._items[int(idx)]
        if tag in item:
            return item[tag]
        raise KeyError(f"未知 tag: {tag}")

    @staticmethod
    def _as_str_list(val) -> List[str]:
        if not val:
            return []
        return [str(x) for x in val]

    def _load_io_pairs(
        self,
        idx: int,
        *,
        source: Optional[str] = None,
    ) -> tuple[List[str], List[str]]:
        src = (source or self.rollout_io_source).strip().lower()
        item = self._items[int(idx)]
        if src == "sample":
            inputs = self._as_str_list(item.get("example_input"))
            outputs = self._as_str_list(item.get("example_output"))
        else:
            inputs = self._as_str_list(item.get("test_input"))
            outputs = self._as_str_list(item.get("test_output"))
        if len(inputs) != len(outputs):
            n = min(len(inputs), len(outputs))
            inputs, outputs = inputs[:n], outputs[:n]
        return inputs, outputs

    def get_io_inputs(self, idx, max_count: int = 10) -> List[str]:
        """rollout / 评测打分：默认 test_input（隐藏测例）。"""
        inputs, _outputs = self._load_io_pairs(idx, source=self.rollout_io_source)
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_io_outputs(self, idx, max_count: int = 10) -> List[str]:
        _inputs, outputs = self._load_io_pairs(idx, source=self.rollout_io_source)
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def get_public_io_inputs(self, idx, max_count: int = 0) -> List[str]:
        """验证 / Public Test bonus：默认题干 Example。"""
        inputs, _outputs = self._load_io_pairs(idx, source=self.public_io_source)
        if max_count > 0:
            return inputs[:max_count]
        return inputs

    def get_public_io_outputs(self, idx, max_count: int = 0) -> List[str]:
        _inputs, outputs = self._load_io_pairs(idx, source=self.public_io_source)
        if max_count > 0:
            return outputs[:max_count]
        return outputs

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)
        for idx in tqdm(range(start, end), desc="CURECodeContests", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
