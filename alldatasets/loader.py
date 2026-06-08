from __future__ import annotations

from pathlib import Path


def datasets_root() -> Path:
    """数据集根目录：优先 ~/lzh/datasets，其次 ~/datasets。"""
    home = Path.home()
    for cand in (home / "lzh" / "datasets", home / "datasets"):
        if cand.is_dir():
            return cand
    return home / "lzh" / "datasets"


def default_dataset_path(dataset: str) -> str:
    """各数据集默认路径（根目录见 datasets_root()）。"""
    root = datasets_root()
    name = (dataset or "").strip().lower()
    rel = {
        "apps": Path("APPS") / "train",
        "codecontestplus": Path("codecontestplus"),
        "ccp": Path("codecontestplus"),
        "codecontests": Path("codecontests"),
        "cc": Path("codecontests"),
        "livecodebench": Path("LiveCodeBench"),
        "lcb": Path("LiveCodeBench"),
        "codeforces": Path("codeforces"),
        "cf": Path("codeforces"),
    }
    if name not in rel:
        raise ValueError(f"未知 dataset={dataset!r}")
    return str(root / rel[name])


def _resolve_existing_path(path: str, *, fallbacks: tuple[str, ...] = ()) -> str:
    p = Path(path).expanduser()
    if p.exists():
        return str(p)
    for fb in fallbacks:
        cand = Path(fb).expanduser()
        if cand.exists():
            return str(cand)
    return str(p)


def resolve_test_subdir(root: Path, *, markers: tuple[str, ...]) -> Path:
    """
    若 root/test/ 下存在 marker 文件，则使用 test 子目录。
    例如 LiveCodeBench/test/test5.jsonl、codeforces/test/test-*.parquet。
    """
    root = root.expanduser()
    test_dir = root / "test"
    if test_dir.is_dir():
        for name in markers:
            if (test_dir / name).is_file():
                return test_dir
    for name in markers:
        if (root / name).is_file():
            return root
    return test_dir if test_dir.is_dir() else root


def _dataset_defaults(name: str) -> dict:
    """训练/验证/评测统一：题干 description 保留 Input/Output/Example。"""
    defaults: dict = {}
    if name in ("apps", "codecontests", "cc", "code_contests"):
        defaults["strip_samples"] = False
    if name in ("codecontests", "cc", "code_contests"):
        defaults["rollout_io_source"] = "tests"
        defaults["public_io_source"] = "sample"
    return defaults


def _resolve_codecontests_path(raw_path: str) -> str:
    """优先使用已解压的 extracted_tasks/ 或含 code_contests-* 的目录。"""
    p = Path(raw_path).expanduser()
    candidates = [p, p / "extracted_tasks"]
    for cand in candidates:
        if cand.is_dir() and any(cand.glob("code_contests-*")):
            return str(cand)
    return str(p)


def load_dataset(dataset: str, path: str = "", **kwargs):
    """
    统一加载数据集。

    dataset:
      - codecontestplus / ccp
      - apps
      - codecontests / cc
      - livecodebench / lcb
      - codeforces / cf
    """
    name = (dataset or "codecontestplus").strip().lower()
    for key, value in _dataset_defaults(name).items():
        kwargs.setdefault(key, value)
    raw_path = (path or "").strip() or default_dataset_path(name)

    if name in ("codecontestplus", "ccp", "code_contest_plus"):
        from alldatasets.codecontestplus import CodeContestPlus

        return CodeContestPlus(raw_path)

    if name == "apps":
        from alldatasets.apps import APPS

        p = _resolve_existing_path(
            raw_path,
            fallbacks=(
                str(datasets_root() / "APPS" / "train"),
                str(Path.home() / "datasets" / "APPS" / "train"),
                str(Path.home() / "get_codeforces_data/APPS/train"),
            ),
        )
        return APPS(p, **kwargs)

    if name in ("codecontests", "cc", "code_contests"):
        from alldatasets.codecontests import CodeContests

        p = _resolve_existing_path(
            raw_path,
            fallbacks=(
                str(datasets_root() / "codecontests"),
                str(Path.home() / "datasets" / "codecontests"),
            ),
        )
        p = _resolve_codecontests_path(p)
        return CodeContests(p, **kwargs)

    if name in ("livecodebench", "lcb"):
        from alldatasets.livecodebench import LiveCodeBench

        p = _resolve_existing_path(
            raw_path,
            fallbacks=(
                str(datasets_root() / "LiveCodeBench"),
                str(Path.home() / "datasets" / "LiveCodeBench"),
                str(Path.home() / "get_codeforces_data/LiveCodeBench"),
            ),
        )
        return LiveCodeBench(p, **kwargs)

    if name in ("codeforces", "cf"):
        from alldatasets.codeforces import CodeForces

        p = _resolve_existing_path(
            raw_path,
            fallbacks=(
                str(datasets_root() / "codeforces"),
                str(Path.home() / "datasets" / "codeforces"),
            ),
        )
        return CodeForces(p, **kwargs)

    raise ValueError(
        f"未知 dataset={dataset!r}，可选: "
        "codecontestplus, apps, codecontests, livecodebench, codeforces"
    )
