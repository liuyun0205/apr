from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Tuple


CSV_COLUMNS = ("key", "num", "lastuse")


def _read_csv(path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows: List[Dict[str, str]] = list(reader)
    return header, rows


def _normalize_header(header: List[str]) -> List[str]:
    h = [x.strip() for x in header if x and x.strip()]
    for c in CSV_COLUMNS:
        if c not in h:
            h.append(c)
    rest = [x for x in h if x not in CSV_COLUMNS]
    return list(CSV_COLUMNS) + rest


def _atomic_write_csv(path: str, header: List[str], rows: List[Dict[str, str]]) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})
    os.replace(tmp_path, path)


def reset_key_csv(csv_path: str, *, dry_run: bool) -> int:
    header, raw_rows = _read_csv(csv_path)
    out_header = _normalize_header(header)

    changed = 0
    total = 0
    for r in raw_rows:
        k = (r.get("key") or "").strip()
        if not k:
            continue
        total += 1
        before_num = (r.get("num") or "").strip()
        before_lastuse = (r.get("lastuse") or "").strip()
        if before_num != "0" or before_lastuse != "":
            changed += 1
        r["num"] = "0"
        r["lastuse"] = ""

    if not dry_run:
        _atomic_write_csv(csv_path, out_header, raw_rows)

    print(f"[key.csv] rows={total} reset={changed}" + (" (dry-run)" if dry_run else ""))
    return 0


def reset_ipring_state(csv_path: str, *, dry_run: bool) -> int:
    state_path = csv_path + ".ipring.json"
    if not os.path.exists(state_path):
        print(f"[ipring] not found, skip: {state_path}")
        return 0

    if dry_run:
        print(f"[ipring] would reset: {state_path} (dry-run)")
        return 0

    tmp = state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, state_path)
    print(f"[ipring] reset: {state_path}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Reset key.csv num/lastuse and (optionally) ipring state")
    p.add_argument("--csv", type=str, default="/home/liuzhihao/文档/key.csv", help="key.csv 路径")
    p.add_argument("--with-ipring", action="store_true", help="同时清空 key.csv.ipring.json")
    p.add_argument("--dry-run", action="store_true", help="只打印将要做的修改，不写文件")
    args = p.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"找不到文件：{csv_path}", file=sys.stderr)
        return 2

    reset_key_csv(csv_path, dry_run=bool(args.dry_run))
    if args.with_ipring:
        reset_ipring_state(csv_path, dry_run=bool(args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

