from __future__ import annotations

from pathlib import Path


def load_dataset(dataset: str, path: str):
    """
    统一加载训练数据集。

    dataset:
      - codecontestplus / ccp
      - apps
    """
    name = (dataset or "codecontestplus").strip().lower()
    p = str(Path(path).expanduser())

    if name in ("codecontestplus", "ccp", "code_contest_plus"):
        from alldatasets.codecontestplus import CodeContestPlus

        return CodeContestPlus(p)

    if name == "apps":
        from alldatasets.apps import APPS

        return APPS(p)

    raise ValueError(
        f"未知 dataset={dataset!r}，可选: codecontestplus, apps"
    )
