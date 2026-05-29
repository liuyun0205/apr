from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


CSV_COLUMNS = ("key", "num", "lastuse")


@dataclass
class KeyRow:
    key: str
    num: int
    lastuse: str


def _now_iso_local() -> str:
    # 生成类似你 csv 里的格式：YYYY-mm-dd HH:MM:SS.mmm
    # 使用本地时间（不带时区），便于直接对照原文件
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(now.microsecond / 1000):03d}"


def _mask_key(k: str) -> str:
    if len(k) <= 10:
        return k[:2] + "***"
    return f"{k[:6]}...{k[-4:]}"


def _read_csv(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows: List[Dict[str, str]] = list(reader)
    return header, rows


def _normalize_header(header: List[str]) -> List[str]:
    # 确保 key/num/lastuse 都存在，且输出顺序稳定
    h = [x.strip() for x in header if x and x.strip()]
    for c in CSV_COLUMNS:
        if c not in h:
            h.append(c)
    # 把关键列放前面，其余列跟在后面
    rest = [x for x in h if x not in CSV_COLUMNS]
    return list(CSV_COLUMNS) + rest


def _parse_rows(rows: List[Dict[str, str]]) -> List[KeyRow]:
    parsed: List[KeyRow] = []
    for r in rows:
        k = (r.get("key") or "").strip()
        if not k:
            continue
        n_raw = (r.get("num") or "0").strip()
        try:
            n = int(float(n_raw))  # 兼容意外的 "190.0"
        except Exception:
            n = 0
        lu = (r.get("lastuse") or "").strip()
        parsed.append(KeyRow(key=k, num=n, lastuse=lu))
    if not parsed:
        raise ValueError("CSV 里没有可用的 key 行（需要列 key/num/lastuse）。")
    return parsed


def _choose_row(rows: List[KeyRow], *, strategy: str, key: Optional[str]) -> int:
    if key:
        for i, r in enumerate(rows):
            if r.key == key:
                return i
        raise ValueError("指定的 key 在 CSV 中不存在。")

    if strategy == "min":
        # num 最小优先；若并列，按 lastuse 最早（空视作最早）
        def score(r: KeyRow) -> Tuple[int, str]:
            return (r.num, r.lastuse or "")

        return min(range(len(rows)), key=lambda i: score(rows[i]))

    if strategy == "max":
        return max(range(len(rows)), key=lambda i: rows[i].num)

    if strategy == "roundrobin":
        # 简单轮询：按 lastuse 最早选（空视作最早）
        return min(range(len(rows)), key=lambda i: rows[i].lastuse or "")

    raise ValueError(f"未知 strategy：{strategy!r}（可选：min/max/roundrobin）")


def _atomic_write_csv(path: str, header: List[str], rows: List[Dict[str, str]]) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})
    os.replace(tmp_path, path)


def main() -> int:
    p = argparse.ArgumentParser(description="Use a key from key.csv and update num/lastuse")
    p.add_argument("--csv", type=str, default="/home/liuzhihao/文档/key.csv", help="key.csv 路径")
    p.add_argument("--strategy", type=str, default="min", choices=["min", "max", "roundrobin"])
    p.add_argument("--key", type=str, default=None, help="指定使用某个 key（完全匹配）")
    p.add_argument("--inc", type=int, default=1, help="本次使用计数增加多少")
    p.add_argument("--dry-run", action="store_true", help="只展示将要修改的内容，不写回文件")
    args = p.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"找不到文件：{csv_path}", file=sys.stderr)
        return 2

    header, raw_rows = _read_csv(csv_path)
    out_header = _normalize_header(header)

    parsed = _parse_rows(raw_rows)
    idx = _choose_row(parsed, strategy=args.strategy, key=args.key)
    chosen = parsed[idx]

    before_num = chosen.num
    chosen.num = max(0, chosen.num + int(args.inc))
    chosen.lastuse = _now_iso_local()

    # 写回 raw_rows：只改被选中的那一行（按 key 精确匹配）
    # 若 CSV 中 key 重复：只更新第一条匹配（和 choose_row 一致）
    updated = False
    for r in raw_rows:
        if (r.get("key") or "").strip() == chosen.key:
            r["num"] = str(chosen.num)
            r["lastuse"] = chosen.lastuse
            updated = True
            break
    if not updated:
        print("内部错误：未找到要更新的行。", file=sys.stderr)
        return 3

    if not args.dry_run:
        _atomic_write_csv(csv_path, out_header, raw_rows)

    print(
        "selected="
        + _mask_key(chosen.key)
        + f" num: {before_num} -> {chosen.num}"
        + f" lastuse -> {chosen.lastuse}"
        + (" (dry-run)" if args.dry_run else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

