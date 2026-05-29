"""sub_test 样本索引映射：对外使用 0 起始的 index，内部通过映射解析真实 id。"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_ROOT = os.path.join(_DIR, "sub_test")
DEFAULT_MAP_PATH = os.path.join(_DIR, "sub_test_index.json")

_ID_FIELDS = ("src_uid", "bug_code_uid", "apr_id")


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
            if isinstance(obj, dict):
                yield obj


def lang_from_path(path: str) -> str:
    """从 Python.jsonl 或 sub_test/Python 推断语言名。"""
    base = os.path.basename(path.rstrip("/"))
    if base.endswith(".jsonl"):
        return os.path.splitext(base)[0]
    return base


def normalize_lang(lang: str) -> str:
    """与 sub_test/<语言>.jsonl 文件名一致（如 PHP、Go、Javascript）。"""
    s = (lang or "").strip()
    if not s:
        return s
    key = s.lower().replace(" ", "")
    aliases = {
        "c": "C",
        "c++": "C++",
        "c#": "C#",
        "php": "PHP",
        "go": "Go",
        "java": "Java",
        "python": "Python",
        "ruby": "Ruby",
        "rust": "Rust",
        "kotlin": "Kotlin",
        "javascript": "Javascript",
        "llvmir": "LLVM IR",
    }
    if key in aliases:
        return aliases[key]
    return s[0].upper() + s[1:].lower() if len(s) > 1 else s.upper()


def build_map(*, data_root: str = DEFAULT_DATA_ROOT) -> Dict[str, Any]:
    """扫描 sub_test/*.jsonl，为每种语言生成 0..n-1 索引。"""
    data_root = os.path.abspath(data_root)
    languages: Dict[str, Any] = {}
    global_idx = 0

    for name in sorted(os.listdir(data_root)):
        if not name.endswith(".jsonl"):
            continue
        lang = os.path.splitext(name)[0]
        path = os.path.join(data_root, name)
        entries: List[Dict[str, Any]] = []
        for local_idx, obj in enumerate(_iter_jsonl(path)):
            entries.append(
                {
                    "index": local_idx,
                    "src_uid": str(obj.get("src_uid") or "").strip(),
                    "bug_code_uid": str(obj.get("bug_code_uid") or "").strip(),
                    "apr_id": str(obj.get("apr_id") or "").strip(),
                }
            )
            global_idx += 1

        languages[lang] = {"count": len(entries), "entries": entries}

    return {
        "version": 1,
        "data_root": data_root,
        "total": global_idx,
        "languages": languages,
    }


def save_map(data: Dict[str, Any], path: str = DEFAULT_MAP_PATH) -> str:
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path


def load_map(path: str = DEFAULT_MAP_PATH) -> Dict[str, Any]:
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"找不到索引映射文件：{path}\n"
            f"请先运行：python {__file__} build"
        )
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "languages" not in data:
        raise ValueError(f"索引映射格式错误：{path}")
    return data


class SubTestIndexMap:
    """按语言维护 index <-> id 双向查询。"""

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._by_lang: Dict[str, List[Dict[str, Any]]] = {}
        self._rev: Dict[str, Dict[str, Dict[str, int]]] = {}
        self._global_entries: List[Dict[str, Any]] = []
        langs = data.get("languages") or {}
        global_idx = 0
        for lang in sorted(langs.keys()):
            block = langs[lang]
            entries = list((block or {}).get("entries") or [])
            self._by_lang[lang] = entries
            rev: Dict[str, Dict[str, int]] = {f: {} for f in _ID_FIELDS}
            for row in entries:
                idx = int(row["index"])
                self._global_entries.append({"global_index": global_idx, "lang": lang, **row})
                global_idx += 1
                for f in _ID_FIELDS:
                    val = str(row.get(f) or "").strip()
                    if val:
                        rev[f][val] = idx
            self._rev[lang] = rev

    @property
    def data_root(self) -> str:
        return str(self._data.get("data_root") or DEFAULT_DATA_ROOT)

    def languages(self) -> List[str]:
        return sorted(self._by_lang.keys())

    def count(self, lang: str) -> int:
        return len(self._by_lang.get(lang, []))

    def entry(self, lang: str, index: int) -> Dict[str, Any]:
        entries = self._by_lang.get(lang)
        if not entries:
            raise KeyError(f"未知语言：{lang}")
        if index < 0 or index >= len(entries):
            raise IndexError(f"{lang} 索引越界：{index}（有效范围 0..{len(entries) - 1}）")
        return dict(entries[index])

    def get_id(self, lang: str, index: int, *, id_field: str = "bug_code_uid") -> str:
        lang = normalize_lang(lang)
        row = self.entry(lang, index)
        val = str(row.get(id_field) or "").strip()
        if not val:
            raise KeyError(f"{lang}[{index}] 缺少字段 {id_field}")
        return val

    def resolve_index(self, lang: str, id_value: str, *, id_field: str = "src_uid") -> int:
        lang = normalize_lang(lang)
        id_value = (id_value or "").strip()
        if not id_value:
            raise ValueError("id 不能为空")
        rev = self._rev.get(lang)
        if not rev:
            raise KeyError(f"未知语言：{lang}")
        if id_field not in _ID_FIELDS:
            for row in self._by_lang[lang]:
                if str(row.get(id_field) or "").strip() == id_value:
                    return int(row["index"])
            raise KeyError(f"{lang} 中未找到 {id_field}={id_value}")
        idx = rev[id_field].get(id_value)
        if idx is None:
            raise KeyError(f"{lang} 中未找到 {id_field}={id_value}")
        return idx

    def global_entry(self, global_index: int) -> Dict[str, Any]:
        if global_index < 0 or global_index >= len(self._global_entries):
            raise IndexError(
                f"全局索引越界：{global_index}（有效范围 0..{len(self._global_entries) - 1}）"
            )
        return dict(self._global_entries[global_index])


_cached: Optional[SubTestIndexMap] = None


def get_map(*, path: str = DEFAULT_MAP_PATH, rebuild: bool = False) -> SubTestIndexMap:
    global _cached
    map_path = os.path.abspath(path)
    if rebuild or not os.path.isfile(map_path):
        save_map(build_map(), map_path)
        _cached = None
    if _cached is None or getattr(_cached, "_path", None) != map_path:
        obj = SubTestIndexMap(load_map(map_path))
        obj._path = map_path  # type: ignore[attr-defined]
        _cached = obj
    return _cached


def resolve_index_to_id(
    lang: str,
    index: int,
    *,
    id_field: str = "src_uid",
    map_path: str = DEFAULT_MAP_PATH,
) -> str:
    return get_map(path=map_path).get_id(lang, index, id_field=id_field)


def _result_stem(name: str) -> tuple[str, str]:
    """返回 (stem, suffix)，suffix 为 .txt 或 .error.txt。"""
    if name.endswith(".error.txt"):
        return name[: -len(".error.txt")], ".error.txt"
    if name.endswith(".txt"):
        return name[: -len(".txt")], ".txt"
    return name, ""


def rename_result_files(
    result_root: str,
    *,
    map_path: str = DEFAULT_MAP_PATH,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """将 results/<run>/<语言>/<bug_code_uid>.txt 重命名为 <index>.txt。"""
    result_root = os.path.abspath(result_root)
    m = get_map(path=map_path)
    stats: Dict[str, Any] = {
        "root": result_root,
        "renamed": 0,
        "skipped": 0,
        "missing": 0,
        "conflicts": 0,
        "errors": [],
    }

    if not os.path.isdir(result_root):
        stats["errors"].append(f"not a directory: {result_root}")
        return stats

    for lang in sorted(os.listdir(result_root)):
        lang_dir = os.path.join(result_root, lang)
        if not os.path.isdir(lang_dir):
            continue
        if lang in ("eval_report.json",) or lang.endswith(".json"):
            continue
        for name in sorted(os.listdir(lang_dir)):
            stem, suffix = _result_stem(name)
            if not suffix:
                continue
            if stem.isdigit():
                stats["skipped"] += 1
                continue
            try:
                idx = m.resolve_index(lang, stem, id_field="bug_code_uid")
            except KeyError:
                stats["missing"] += 1
                stats["errors"].append(f"{lang}/{name}: bug_code_uid not in map")
                continue
            new_name = f"{idx}{suffix}"
            src = os.path.join(lang_dir, name)
            dst = os.path.join(lang_dir, new_name)
            if os.path.exists(dst) and os.path.abspath(src) != os.path.abspath(dst):
                stats["conflicts"] += 1
                stats["errors"].append(f"{lang}/{name} -> {new_name}: target exists")
                continue
            if dry_run:
                print(f"would rename: {lang}/{name} -> {new_name}")
            else:
                os.rename(src, dst)
            stats["renamed"] += 1
    return stats


def _cmd_rename_results(args: argparse.Namespace) -> int:
    roots = args.result_dir or []
    if not roots:
        print("请指定至少一个结果目录", file=sys.stderr)
        return 2
    total_renamed = 0
    for root in roots:
        st = rename_result_files(root, map_path=args.map, dry_run=args.dry_run)
        print(
            f"{st['root']}: renamed={st['renamed']} skipped={st['skipped']} "
            f"missing={st['missing']} conflicts={st['conflicts']}"
        )
        if st["errors"]:
            for err in st["errors"][:20]:
                print(f"  ! {err}")
            if len(st["errors"]) > 20:
                print(f"  ... 另有 {len(st['errors']) - 20} 条")
        total_renamed += int(st["renamed"])
    return 0 if total_renamed or args.dry_run else 1


def _cmd_build(args: argparse.Namespace) -> int:
    data = build_map(data_root=args.data_root)
    out = save_map(data, args.map)
    total = data.get("total", 0)
    langs = ", ".join(f"{k}({v['count']})" for k, v in sorted(data["languages"].items()))
    print(f"已写入 {out}")
    print(f"共 {total} 条；各语言：{langs}")
    return 0


def _cmd_lookup(args: argparse.Namespace) -> int:
    m = get_map(path=args.map)
    if args.global_index is not None:
        row = m.global_entry(args.global_index)
        print(json.dumps(row, ensure_ascii=False))
        return 0
    if not args.lang:
        print("需要 --lang 或 --global-index", file=sys.stderr)
        return 2
    if args.index is not None:
        row = m.entry(args.lang, args.index)
        if args.id_field:
            print(row.get(args.id_field, ""))
        else:
            print(json.dumps(row, ensure_ascii=False))
        return 0
    if args.id and args.id_field:
        idx = m.resolve_index(args.lang, args.id, id_field=args.id_field)
        print(idx)
        return 0
    print("需要 --index 或 (--id 与 --id-field)", file=sys.stderr)
    return 2


def main() -> int:
    p = argparse.ArgumentParser(description="sub_test 0 起始索引与真实 id 的映射")
    p.add_argument("--map", type=str, default=DEFAULT_MAP_PATH, help="映射 JSON 路径")
    p.add_argument("--data-root", type=str, default=DEFAULT_DATA_ROOT, help="sub_test 目录")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="从 jsonl 重新生成映射文件")
    b.set_defaults(func=_cmd_build)

    l = sub.add_parser("lookup", help="index -> id，或 id -> index")
    l.add_argument("--lang", type=str, default=None)
    l.add_argument("--index", type=int, default=None)
    l.add_argument("--global-index", type=int, default=None, dest="global_index")
    l.add_argument("--id", type=str, default=None)
    l.add_argument("--id-field", type=str, default="src_uid")
    l.set_defaults(func=_cmd_lookup)

    r = sub.add_parser("rename-results", help="将结果目录中的 hash 文件名改为 index.txt")
    r.add_argument(
        "result_dir",
        nargs="+",
        help="结果目录，如 results/direct_GPT_5-mini",
    )
    r.add_argument("--dry-run", action="store_true", help="只打印将要执行的重命名")
    r.set_defaults(func=_cmd_rename_results)

    args = p.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
