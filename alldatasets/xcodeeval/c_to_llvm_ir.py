"""将 sub_test/C.jsonl 中的 buggy C 编译为 LLVM IR，写入 LLVM IR-C.jsonl 与 LLVM IR-C/ 目录。"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_C_JSONL = os.path.join(_DIR, "sub_test", "C.jsonl")
DEFAULT_OUT_JSONL = os.path.join(_DIR, "sub_test", "LLVM IR-C.jsonl")
DEFAULT_OUT_DIR = os.path.join(_DIR, "sub_test", "LLVM IR-C")

# 竞赛 C 代码常见非标准用法：gets、缺省函数返回类型等
_COMPILE_PREAMBLE = """\
#line 1 "apr_snippet.c"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
static char* apr_gets(char* s) { return fgets(s, 1 << 20, stdin); }
#define gets apr_gets
"""


def _iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                raise ValueError(f"jsonl 解析失败：{path}:{line_no}") from e
            if isinstance(obj, dict):
                yield obj


def compile_c_to_llvm(
    code: str,
    *,
    clang: str = "clang",
    std: str = "gnu11",
    extra_flags: Optional[List[str]] = None,
) -> Tuple[Optional[str], str]:
    """返回 (llvm_ir_text, status)，status 为 ok 或错误摘要。"""
    flags = [
        clang,
        "-S",
        "-emit-llvm",
        f"-std={std}",
        "-O0",
        "-Wno-everything",
        "-Wno-implicit-int",
        "-Wno-implicit-function-declaration",
    ]
    if extra_flags:
        flags.extend(extra_flags)

    with tempfile.TemporaryDirectory(prefix="apr_llvm_c_") as tmp:
        src = os.path.join(tmp, "snippet.c")
        ll = os.path.join(tmp, "snippet.ll")
        with open(src, "w", encoding="utf-8") as f:
            f.write(_COMPILE_PREAMBLE)
            f.write(code)

        proc = subprocess.run(
            [*flags, "-o", ll, src],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0 and os.path.isfile(ll):
            with open(ll, "r", encoding="utf-8") as f:
                return f.read(), "ok"

        err = (proc.stderr or proc.stdout or "").strip()
        if err:
            err = err.splitlines()[-1][:500]
        return None, err or f"clang exit {proc.returncode}"


def _failed_ir_placeholder(index: int, message: str) -> str:
    msg = message.replace("\n", " ").strip()
    return (
        f"; compile_failed index={index}\n"
        f"; {msg}\n"
        "; (original C could not be lowered to LLVM IR)\n"
    )


def convert_dataset(
    *,
    c_jsonl: str,
    out_jsonl: str,
    out_dir: str,
    clang: str = "clang",
    write_ll_files: bool = True,
) -> Dict[str, Any]:
    os.makedirs(os.path.dirname(out_jsonl) or ".", exist_ok=True)
    if write_ll_files:
        os.makedirs(out_dir, exist_ok=True)

    stats = {"total": 0, "ok": 0, "failed": 0, "failed_indices": []}
    rows: List[Dict[str, Any]] = []

    for index, item in enumerate(_iter_jsonl(c_jsonl)):
        stats["total"] += 1
        code = item.get("bug_source_code") or ""
        if not isinstance(code, str) or not code.strip():
            ir = _failed_ir_placeholder(index, "empty bug_source_code")
            stats["failed"] += 1
            stats["failed_indices"].append(index)
        else:
            ir_text, status = compile_c_to_llvm(code, clang=clang)
            if ir_text is not None:
                ir = ir_text
                stats["ok"] += 1
            else:
                ir = _failed_ir_placeholder(index, status)
                stats["failed"] += 1
                stats["failed_indices"].append(index)

        if write_ll_files:
            ll_path = os.path.join(out_dir, f"{index}.ll")
            with open(ll_path, "w", encoding="utf-8") as f:
                f.write(ir)
            if isinstance(code, str) and code.strip():
                src_path = os.path.join(out_dir, f"{index}.c")
                with open(src_path, "w", encoding="utf-8") as f:
                    f.write(code)

        row = dict(item)
        row["lang"] = "LLVM IR"
        row["lang_cluster"] = "LLVM IR-C"
        row["original_source_code"] = code if isinstance(code, str) else ""
        row["bug_source_code"] = ir
        row["llvm_ir_from"] = "C"
        row["llvm_ir_index"] = index
        rows.append(row)

    with open(out_jsonl, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    stats["out_jsonl"] = os.path.abspath(out_jsonl)
    stats["out_dir"] = os.path.abspath(out_dir) if write_ll_files else ""
    return stats


def write_source_files(*, src_jsonl: str, out_dir: str, ext: str = ".c") -> int:
    """仅把原始源码写入 out_dir/<index><ext>，不重新编译 IR。"""
    os.makedirs(out_dir, exist_ok=True)
    n = 0
    for index, item in enumerate(_iter_jsonl(src_jsonl)):
        code = item.get("bug_source_code") or ""
        if not isinstance(code, str) or not code.strip():
            continue
        with open(os.path.join(out_dir, f"{index}{ext}"), "w", encoding="utf-8") as f:
            f.write(code)
        n += 1
    return n


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="将 sub_test C 转为 LLVM IR 数据集")
    p.add_argument("--c-jsonl", type=str, default=DEFAULT_C_JSONL)
    p.add_argument("--out-jsonl", type=str, default=DEFAULT_OUT_JSONL)
    p.add_argument("--out-dir", type=str, default=DEFAULT_OUT_DIR)
    p.add_argument("--clang", type=str, default="clang")
    p.add_argument("--no-ll-files", action="store_true", help="不写入 LLVM IR-C/<index>.ll")
    p.add_argument("--sources-only", action="store_true", help="仅写入 LLVM IR-C/<index>.c，不编译 IR")
    p.add_argument("--rebuild-map", action="store_true", help="转换后重建 sub_test_index.json")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if not os.path.isfile(args.c_jsonl):
        print(f"找不到：{args.c_jsonl}", file=sys.stderr)
        return 2

    if args.sources_only:
        n = write_source_files(src_jsonl=args.c_jsonl, out_dir=args.out_dir, ext=".c")
        print(f"已写入 {n} 个 .c -> {args.out_dir}")
        return 0

    if shutil.which(args.clang) is None:
        print(f"找不到编译器：{args.clang}", file=sys.stderr)
        return 2

    print(f"输入: {args.c_jsonl}")
    print(f"输出 jsonl: {args.out_jsonl}")
    if not args.no_ll_files:
        print(f"输出目录: {args.out_dir}")

    stats = convert_dataset(
        c_jsonl=args.c_jsonl,
        out_jsonl=args.out_jsonl,
        out_dir=args.out_dir,
        clang=args.clang,
        write_ll_files=not args.no_ll_files,
    )
    print(f"完成: total={stats['total']} ok={stats['ok']} failed={stats['failed']}")
    if stats["failed_indices"]:
        print(f"编译失败行号: {stats['failed_indices']}")

    if args.rebuild_map:
        from sub_test_map import build_map, save_map, DEFAULT_MAP_PATH

        save_map(build_map())
        print(f"已重建 {DEFAULT_MAP_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
