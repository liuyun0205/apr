from pathlib import Path
import pandas as pd
from tqdm import tqdm


class CodeContestPlus:
    def __init__(self, path="~/lzh/datasets/codecontestplus"):
        self.path = Path(path).expanduser()

        if self.path.is_file():
            self.df = pd.read_parquet(self.path)
        else:
            files = sorted(self.path.glob("part-*.parquet"))

            if not files:
                raise FileNotFoundError(f"没有找到 parquet 文件: {self.path}")

            self.df = pd.concat(
                [pd.read_parquet(f) for f in files],
                ignore_index=True
            )

        if "idx" not in self.df.columns:
            self.df.insert(0, "idx", range(len(self.df)))

        self.df.set_index("idx", inplace=True, drop=False)

    def get(self, idx):
        try:
            return self.df.loc[idx]
        except KeyError:
            raise IndexError(f"idx={idx} 不存在")

    def get_by_tag(self, tag, idx):
        row = self.get(idx)

        if tag not in self.df.columns:
            raise KeyError(f"未知 tag: {tag}")

        return row[tag]

    def foreach(self, func, start=0, end=None):
        if end is None:
            end = len(self.df)

        for idx in tqdm(range(start, end),desc="Processing",unit="problem"):
            print(idx)
            func(idx, self.get_by_tag('description',idx))
