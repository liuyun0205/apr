from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Query xcodeeval jsonl by id and tag")
    p.add_argument("--path", type=str, required=True, help="jsonl 路径，例如 alldatasets/xcodeeval/sub_test/C.jsonl")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=str, default=None, help="要匹配的 id 值")
    g.add_argument(
        "--index",
        type=int,
        default=None,
        help="0 起始行号（通过 sub_test_index.json 解析为 id；需配合 --path 推断语言）",
    )
    p.add_argument("--id-field", type=str, default="src_uid", help="id 字段名（默认 src_uid）")
    p.add_argument("--tag", type=str, default=None, help="可选：tags 里必须包含该 tag（精确匹配）")
    p.add_argument(
        "--field",
        type=str,
        default=None,
        help="输出哪个字段；不填则输出整条 JSON（例如 bug_source_code / tags / difficulty）",
    )
    p.add_argument("--first", action="store_true", help="找到第一条就返回（默认会返回所有匹配项）")
    return p.parse_args()


def _iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                raise ValueError(f"jsonl 解析失败：{path}:{line_no}") from e
            if not isinstance(obj, dict):
                continue
            yield obj


def _has_tag(obj: Dict[str, Any], tag: str) -> bool:
    tags = obj.get("tags")
    if tags is None:
        return False
    if isinstance(tags, list):
        return tag in tags
    if isinstance(tags, str):
        # 兼容偶发的字符串 tags
        return tag == tags
    return False


def main() -> int:
    args = _parse_args()
    path = args.path
    if not os.path.exists(path):
        print(f"找不到文件：{path}", file=sys.stderr)
        return 2

    matches: List[Dict[str, Any]] = []
    if args.index is not None:
        from sub_test_map import get_map, lang_from_path

        lang = lang_from_path(path)
        row = get_map().entry(lang, args.index)
        target_id = str(row.get(args.id_field) or "").strip()
        print(f"index {args.index} ({lang}) -> {args.id_field}={target_id}", file=sys.stderr)
        for i, obj in enumerate(_iter_jsonl(path)):
            if i != args.index:
                continue
            if args.tag and not _has_tag(obj, args.tag):
                break
            matches.append(obj)
            break
    else:
        target_id = args.id
        for obj in _iter_jsonl(path):
            if str(obj.get(args.id_field, "")) != target_id:
                continue
            if args.tag and not _has_tag(obj, args.tag):
                continue
            matches.append(obj)
            if args.first:
                break

    if not matches:
        print("未找到匹配项", file=sys.stderr)
        return 1

    if args.field is None:
        for m in matches:
            print(json.dumps(m, ensure_ascii=False))
        return 0

    for m in matches:
        val = m.get(args.field)
        if isinstance(val, (dict, list)):
            print(json.dumps(val, ensure_ascii=False))
        elif val is None:
            print("")
        else:
            print(str(val))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

