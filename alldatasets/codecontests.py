"""
CodeContests（open-thoughts/CodeContests，Harbor 格式）。

支持两种路径：
  1. 已解压目录：含 code_contests-0000/ 等子目录，每题有 instruction.md + tests/test_data.json
  2. parquet：tasks.parquet 或目录下含 tasks.parquet（按需解压到 extracted_tasks/ 缓存）

与 APPS 相同接口：get / get_by_tag / get_io_inputs / get_io_outputs / foreach。
"""
from __future__ import annotations

import io
import json
import re
import tarfile
from pathlib import Path, PurePosixPath
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

from alldatasets.apps import (
    _read_text,
    extract_pure_problem,
    parse_input_output_inputs,
    parse_input_output_outputs,
)

_HARBOR_SECTION_MARKERS = (
    "## contest information",
    "## task",
    "## test cases",
)

_PROB_RE = re.compile(r"^code_contests-\d+$")
_SAMPLE_OUT_STOP = re.compile(
    r"(?im)^\s*(?:explanation|##\s|note\s*:|\Z)"
)


def parse_instruction_sample_io(text: str) -> tuple[List[str], List[str]]:
    """
    从 instruction.md 题干 Example 段提取样例 input/output（单组或多组）。
    不用 tests/test_data.json 里的隐藏测例。
    """
    if not (text or "").strip():
        return [], []

    chunk = text
    m_example = re.search(r"(?im)^\s*example\b", text)
    if m_example:
        chunk = text[m_example.start() :]

    m_in = re.search(r"(?is)(?:^|\n)\s*input\s*:?\s*\n(.*)", chunk)
    if not m_in:
        return [], []
    after_in = m_in.group(1)

    m_out = re.search(r"(?is)\n\s*output\s*:?\s*\n(.*)", after_in)
    if not m_out:
        return [], []

    inp_block = after_in[: m_out.start()].strip("\n")
    out_block = _SAMPLE_OUT_STOP.split(m_out.group(1), maxsplit=1)[0].strip("\n")

    if not inp_block.strip() or not out_block.strip():
        return [], []
    return [inp_block], [out_block]


def extract_instruction_description(text: str, *, strip_samples: bool = False) -> str:
    """从 Harbor instruction.md 提取题面；默认保留 Input/Output/Constraints/Example。"""
    lines = text.splitlines()
    body_lines: List[str] = []
    for line in lines:
        low = line.strip().lower()
        if low.startswith("## ") and any(m in low for m in _HARBOR_SECTION_MARKERS):
            break
        body_lines.append(line)

    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    if body_lines and body_lines[0].strip().startswith("#"):
        body_lines.pop(0)
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    if body_lines and body_lines[0].strip().lower().startswith("## problem description"):
        body_lines.pop(0)
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)

    body = "\n".join(body_lines).strip()
    if strip_samples and body:
        body = extract_pure_problem(body)
    return body


def _sanitize_tar_member_name(name: str) -> str:
    p = PurePosixPath(name)
    parts = [part for part in p.parts if part not in ("..", ".", "")]
    return str(PurePosixPath(*parts)) if parts else ""


def _extract_tar_to_dir(archive_bytes: bytes, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO(archive_bytes)
    with tarfile.open(fileobj=buf, mode="r:*") as tf:
        for member in tf.getmembers():
            member_name = _sanitize_tar_member_name(member.name)
            if not member_name or member_name.endswith("/"):
                (dest_dir / member_name).mkdir(parents=True, exist_ok=True)
                continue
            if ".snapshot" in PurePosixPath(member_name).parts:
                continue
            target = dest_dir / member_name
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.isfile():
                src = tf.extractfile(member)
                if src is None:
                    continue
                target.write_bytes(src.read())
            elif member.isdir():
                target.mkdir(parents=True, exist_ok=True)


class CodeContests:
    """与 APPS 相同接口：get / get_by_tag / get_io_inputs / foreach。"""

    TAG_FILES = {
        "instruction": "instruction.md",
        "test_data": "tests/test_data.json",
    }

    def __init__(
        self,
        path: str = "~/datasets/codecontests",
        *,
        strip_samples: bool = False,
        require_instruction: bool = True,
        cache_dir: str = "",
        io_source: str = "",
        rollout_io_source: str = "tests",
        public_io_source: str = "sample",
    ):
        self.path = Path(path).expanduser()
        self.strip_samples = strip_samples
        self.require_instruction = require_instruction
        legacy = (io_source or "").strip().lower()
        self.rollout_io_source = (
            (rollout_io_source or legacy or "tests").strip().lower()
        )
        self.public_io_source = (
            (public_io_source or "sample").strip().lower()
        )
        for name, src in (
            ("rollout_io_source", self.rollout_io_source),
            ("public_io_source", self.public_io_source),
        ):
            if src not in ("tests", "sample"):
                raise ValueError(
                    f"{name} 须为 tests 或 sample，收到: {src!r}"
                )
        # 兼容旧字段
        self.io_source = self.rollout_io_source

        if self.path.is_file() and self.path.suffix == ".parquet":
            self._mode = "parquet"
            self.parquet_path = self.path
            self.root = self.path.parent
        elif (self.path / "tasks.parquet").is_file():
            self._mode = "parquet"
            self.parquet_path = self.path / "tasks.parquet"
            self.root = self.path
        elif any(_PROB_RE.match(p.name) for p in self.path.iterdir() if p.is_dir()):
            self._mode = "dir"
            self.parquet_path = None
            self.root = self.path
        elif (self.path / "extracted_tasks").is_dir() and any(
            _PROB_RE.match(p.name)
            for p in (self.path / "extracted_tasks").iterdir()
            if p.is_dir()
        ):
            self._mode = "dir"
            self.parquet_path = None
            self.root = self.path / "extracted_tasks"
        else:
            raise FileNotFoundError(
                f"CodeContests 路径无效: {self.path}\n"
                "需要以下之一：\n"
                "  - 含 code_contests-0000/ 的解压目录\n"
                "  - 含 tasks.parquet 的目录\n"
                "  - tasks.parquet 文件本身"
            )

        self.cache_dir = (
            Path(cache_dir).expanduser()
            if cache_dir
            else self.root / "extracted_tasks"
        )
        self._parquet_table = None
        self._parquet_paths: Optional[List[str]] = None
        self._parquet_binaries: Optional[List[bytes]] = None

        if self._mode == "parquet":
            import pyarrow.parquet as pq

            self._parquet_table = pq.read_table(str(self.parquet_path))
            self._parquet_paths = self._parquet_table.column("path").to_pylist()
            self._parquet_binaries = self._parquet_table.column("task_binary").to_pylist()

        rows = self._build_index()
        if not rows:
            raise FileNotFoundError(f"未在 {self.path} 下找到有效 CodeContests 题目")

        self.df = pd.DataFrame(rows)
        self.df.set_index("idx", inplace=True, drop=False)

    def _build_index(self) -> List[dict]:
        rows: List[dict] = []

        if self._mode == "dir":
            prob_dirs = sorted(
                p for p in self.root.iterdir()
                if p.is_dir() and _PROB_RE.match(p.name)
            )
            for prob_dir in prob_dirs:
                row = self._row_from_dir(prob_dir)
                if row is not None:
                    row["idx"] = len(rows)
                    rows.append(row)
            return rows

        assert self._parquet_paths is not None
        for i, rel_path in enumerate(self._parquet_paths):
            prob_id = Path(str(rel_path)).name
            if not _PROB_RE.match(prob_id):
                continue
            prob_dir = self.cache_dir / prob_id
            if prob_dir.is_dir():
                row = self._row_from_dir(prob_dir, prob_id=prob_id)
                if row is None:
                    continue
            else:
                row = {
                    "id": prob_id,
                    "dir": str(prob_dir),
                    "description": "",
                    "instruction_raw": "",
                }
            row["idx"] = len(rows)
            row["parquet_row"] = i
            rows.append(row)
        return rows

    def _row_from_dir(self, prob_dir: Path, *, prob_id: str = "") -> Optional[dict]:
        inst_path = prob_dir / "instruction.md"
        if self.require_instruction and not inst_path.exists():
            return None

        instruction_raw = _read_text(inst_path) if inst_path.exists() else ""
        description = (
            extract_instruction_description(instruction_raw, strip_samples=self.strip_samples)
            if instruction_raw
            else ""
        )
        return {
            "id": prob_id or prob_dir.name,
            "description": description,
            "instruction_raw": instruction_raw,
            "dir": str(prob_dir),
        }

    def _ensure_extracted(self, idx: int) -> Path:
        row = self.get(idx)
        prob_dir = Path(str(self.df.at[idx, "dir"]))
        if prob_dir.is_dir() and (prob_dir / "instruction.md").exists():
            return prob_dir

        if self._mode != "parquet":
            raise FileNotFoundError(f"题目目录不存在: {prob_dir}")

        assert self._parquet_paths is not None
        assert self._parquet_binaries is not None
        prob_id = row["id"]
        parquet_idx = int(row.get("parquet_row", -1))
        if parquet_idx < 0:
            try:
                parquet_idx = self._parquet_paths.index(prob_id)
            except ValueError:
                parquet_idx = self._parquet_paths.index(f"code_contests/{prob_id}")

        _extract_tar_to_dir(bytes(self._parquet_binaries[parquet_idx]), prob_dir)

        instruction_raw = _read_text(prob_dir / "instruction.md")
        description = extract_instruction_description(
            instruction_raw, strip_samples=self.strip_samples
        )
        self.df.at[idx, "description"] = description
        self.df.at[idx, "instruction_raw"] = instruction_raw
        return prob_dir

    def get(self, idx):
        try:
            return self.df.loc[idx]
        except KeyError:
            raise IndexError(f"idx={idx} 不存在")

    def get_by_tag(self, tag, idx):
        row = self.get(idx)

        if tag in self.df.columns:
            val = row[tag]
            if tag == "description" and not str(val).strip():
                self._ensure_extracted(idx)
                return self.df.at[idx, "description"]
            return val

        if tag == "description":
            if not str(row["description"]).strip():
                self._ensure_extracted(idx)
            return self.df.at[idx, "description"]

        prob_dir = self._ensure_extracted(idx)

        if tag in self.TAG_FILES:
            fpath = prob_dir / self.TAG_FILES[tag]
            if not fpath.exists():
                raise FileNotFoundError(f"缺少文件: {fpath}")
            return _read_text(fpath)

        raise KeyError(f"未知 tag: {tag}")

    def problem_dir(self, idx) -> Path:
        return self._ensure_extracted(idx)

    def _load_io_pairs(
        self,
        idx: int,
        *,
        source: Optional[str] = None,
    ) -> tuple[List[str], List[str]]:
        src = (source or self.rollout_io_source).strip().lower()
        prob_dir = self.problem_dir(idx)
        if src == "sample":
            inst_path = prob_dir / "instruction.md"
            if not inst_path.exists():
                return [], []
            return parse_instruction_sample_io(_read_text(inst_path))

        io_path = prob_dir / "tests" / "test_data.json"
        if not io_path.exists():
            return [], []
        text = _read_text(io_path)
        return (
            parse_input_output_inputs(text),
            parse_input_output_outputs(text),
        )

    def get_io_inputs(self, idx, max_count: int = 10) -> List[str]:
        """rollout / 评测打分：默认 tests/test_data.json。"""
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

        for idx in tqdm(range(start, end), desc="CodeContests", unit="problem"):
            func(idx, self.get_by_tag("description", idx))
