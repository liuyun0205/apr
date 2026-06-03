from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402


@dataclass(frozen=True)
class PatchedRow:
    idx: Optional[int]
    id: str
    original_response: str
    patched_code: str


def _try_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if not x:
        return None
    try:
        return int(x)
    except Exception:
        return None


def patch_code_to_return_gen_random_input(code: str) -> str:
    """
    把常见的“main()里 print(gen_random_input())”改成“return gen_random_input()”。

    说明：
    - 这是启发式的文本补丁（不做 AST 解析），覆盖你贴的那种模板。
    - 如果找不到 main() 或 gen_random_input()，则原样返回。
    """
    src = code or ""
    if "def main" not in src or "gen_random_input" not in src:
        return src

    # 最稳的目标：把 print(gen_random_input()) 直接替换为 return gen_random_input()
    patched = src.replace("print(gen_random_input())", "return gen_random_input()")

    # 如果 main() 里还有 for 循环打印多次（如 NUM_SAMPLES），通常也一起干掉：
    # 这块尽量保守：仅在替换后仍含有 NUM_SAMPLES/for _ in range 时，把它们保持不动也不影响，
    # 但返回值会被循环覆盖不了，所以建议用户的模板直接用单次 return。
    return patched


def patch_csv_responses(
    csv_path: str | Path,
    *,
    out_csv_path: str | Path | None = None,
) -> List[PatchedRow]:
    """
    读取 CSV（需要至少有 response 列；可选 idx/id 列），对每行 response 做：
    - utils.clean_code 提取代码块
    - patch_code_to_return_gen_random_input 打补丁
    返回补丁后的代码列表；可选写出一个新 CSV。
    """
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    rows: List[PatchedRow] = []
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "response" not in reader.fieldnames:
            raise ValueError(f"CSV 缺少 response 列：{p}")

        for r in reader:
            resp = (r.get("response") or "").strip()
            code = utils.clean_code(resp)
            patched = patch_code_to_return_gen_random_input(code)
            rows.append(
                PatchedRow(
                    idx=_try_int(r.get("idx") or ""),
                    id=(r.get("id") or "").strip(),
                    original_response=resp,
                    patched_code=patched,
                )
            )

    if out_csv_path is not None:
        outp = Path(out_csv_path)
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["idx", "id", "patched_code"])
            for row in rows:
                w.writerow([row.idx if row.idx is not None else "", row.id, row.patched_code])

    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="input_trigger_result.csv")
    ap.add_argument(
        "--out-csv",
        default="input_trigger_result.patched.csv",
        help="把 patched_code 写到这个 CSV（默认写到同目录新文件）",
    )
    args = ap.parse_args()

    patch_csv_responses(args.csv, out_csv_path=args.out_csv)

