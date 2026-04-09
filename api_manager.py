import os

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional
from filelock import FileLock, Timeout


class ExcelKeyTable:
    def __init__(self, path=Path(r"C:\Users\18240\OneDrive\shared_keys\key.xlsx"), sheet_name: str = "Sheet1"):
        self.path = path
        self.sheet_name = sheet_name
        self.lock_path = str(self.path) + ".lock"  # 锁文件
        self.df: Optional[pd.DataFrame] = None
        self.envkey="OPENAI_API_KEY"
    def add(self,key):
        def do():
            if not self.df.empty and (self.df["key"] == key).any():
                # 已经有这个 key，就不重复加
                return
            new_row = {"key": key, "num": 0, "lastuse": None}
            self.df = pd.concat(
                [self.df, pd.DataFrame([new_row])],
                ignore_index=True
            )

        return self._with_lock(do)

    def load(self) -> pd.DataFrame:
        if self.path.exists():
            df = pd.read_excel(self.path, sheet_name=self.sheet_name, engine="openpyxl")
            for col in ["key", "num", "lastuse"]:
                if col not in df.columns:
                    df[col] = None
            df["num"] = df["num"].fillna(0).astype(int)
            return df
        else:
            df = pd.DataFrame(columns=["key", "num", "lastuse"])
            self.save(df)
            return df

    def save(self, df: Optional[pd.DataFrame] = None):
        if df is None:
            df = self.df
        df.to_excel(self.path, sheet_name=self.sheet_name, index=False, engine="openpyxl")
    def _with_lock(self, func, timeout: float = 10.0):
        """所有会读写 Excel 的操作都包在这个锁里"""
        lock = FileLock(self.lock_path)
        try:
            with lock.acquire(timeout=timeout):
                self.df = self.load()
                result = func()
                self.save()
                return result
        except Timeout:
            raise RuntimeError(f"获取 {self.path} 锁超时（>{timeout}s）")

    def next_key(self, daily_quota: int = 190) -> Optional[str]:
        """
        轮到下一个有额度的 key：
        - 每个 key 每天最多 daily_quota 次（默认 200）
        - 按 lastuse 从最久没用的 key 开始轮换
        - 自动在天变更时把 num 重置为 0
        """

        def do():
            # df 已在 _with_lock 里通过 self.load() 保证存在
            if self.df is None or self.df.empty:
                return None

            # 确保 lastuse 是 datetime 类型
            self.df["lastuse"] = pd.to_datetime(self.df["lastuse"], errors="coerce")

            today = datetime.now().date()

            # 今天之前用过的 / 从没用过的，num 归 0
            mask_reset = self.df["lastuse"].isna() | (self.df["lastuse"].dt.date != today)
            if mask_reset.any():
                self.df.loc[mask_reset, "num"] = 0

            # 还有额度的 key：num < daily_quota
            candidates = self.df[self.df["num"] < daily_quota].copy()
            print(candidates)
            if candidates.empty:
                # 今天所有 key 都打满配额了
                return None

            # 从 lastuse 最早的开始（NaT 视为最早）
            candidates = candidates.sort_values("lastuse", na_position="first")
            idx = candidates.index[0]
            key = self.df.at[idx, "key"]

            os.environ[self.envkey]=key

            return key

        return self._with_lock(do)

    def keyuse(self, key: str) -> bool:
        """
        指定 key 被使用了一次：
        - 如果不存在这个 key，则自动插入一条记录（num=1, lastuse=现在）
        - 如果存在：
            * 如果 lastuse 不是今天 -> num 先重置为 0
            * 然后 num += 1, lastuse = 现在
        """

        def do():
            # df 已在 _with_lock 里 load 好
            if self.df is None or self.df.empty:
                # 如果整个表是空的，就直接插入新 key
                now = datetime.now()
                self.df = pd.DataFrame(
                    [{"key": key, "num": 1, "lastuse": now}],
                    columns=["key", "num", "lastuse"]
                )
                return True

            # 确保 lastuse 是 datetime 类型
            self.df["lastuse"] = pd.to_datetime(self.df["lastuse"], errors="coerce")

            now = datetime.now()
            today = now.date()

            mask = self.df["key"] == key
            if not mask.any():
                # 没有这个 key，就新加一行
                new_row = {"key": key, "num": 1, "lastuse": now}
                self.df = pd.concat(
                    [self.df, pd.DataFrame([new_row])],
                    ignore_index=True
                )
                return True

            # 找到这个 key 的那一行（如果有多行，只取第一行）
            idx = self.df[mask].index[0]

            last = self.df.at[idx, "lastuse"]
            num = self.df.at[idx, "num"]
            num = int(num) if pd.notna(num) else 0

            # 如果上次使用不是今天，就把 num 归零（每天重新计数）
            if pd.notna(last) and last.date() != today:
                num = 0

            self.df.at[idx, "num"] = num + 1
            self.df.at[idx, "lastuse"] = now
            return True

        return self._with_lock(do)

    def keycheck(self, key: str=None, daily_quota: int =190):
        def do():
            nonlocal key
            if key is None:
                key = os.environ.get(self.envkey, "")

            # 表为空的情况，直接新建一行
            if self.df is None or self.df.empty:
                self.df = pd.DataFrame(
                    [{"key": key, "num": 0, "lastuse": None}],
                    columns=["key", "num", "lastuse"]
                )
                return True, 0, None

            # 统一转换 lastuse 为 datetime
            self.df["lastuse"] = pd.to_datetime(self.df["lastuse"], errors="coerce")

            now = datetime.now()
            today = now.date()

            mask = self.df["key"] == key
            if not mask.any():
                # 没有这个 key，自动插入一条
                self.df = pd.concat(
                    [self.df,
                     pd.DataFrame([{"key": key, "num": 0, "lastuse": None}])],
                    ignore_index=True
                )
                return True, 0, None

            idx = self.df[mask].index[0]
            last = self.df.at[idx, "lastuse"]
            num_val = self.df.at[idx, "num"]
            num = int(num_val) if pd.notna(num_val) else 0

            # 如果 lastuse 是今天 -> 检查是否超额
            if pd.notna(last) and last.date() == today:
                if num >= daily_quota:
                    print("超限！！！！！")
                    # 今天已经 >= daily_quota 次，不能再用了
                    return False, num, last
                else:
                    # 今天还没用满，只是查询不自增
                    return True, num, last
            else:
                # 不是今天：视为新的一天，把 num 清 0
                self.df.at[idx, "num"] = 0
                # lastuse 可以保留旧值方便你看历史
                return True, 0, last

        return self._with_lock(do)

    def ensure_today_env_key(self,env_name: str = "OPENAI_API_KEY",daily_quota: int = 200):
        def do():
            # self.df 已由 _with_lock 负责 load
            if self.df is None or self.df.empty:
                return None

            # 统一把 lastuse 转成 datetime
            self.df["lastuse"] = pd.to_datetime(self.df["lastuse"], errors="coerce")

            now = datetime.now()
            today = now.date()

            # 当前环境中的 key
            cur =os.environ.get(env_name, "")

            # ---------- 1. 先尝试用当前环境中的 key ----------
            if cur is not None:
                # 只接受在 Excel 表里存在的 key
                mask = self.df["key"] == cur
                if mask.any():
                    idx = self.df[mask].index[0]
                    last = self.df.at[idx, "lastuse"]
                    num_val = self.df.at[idx, "num"]
                    num = int(num_val) if pd.notna(num_val) else 0

                    if pd.notna(last) and last.date() == today:
                        # 今天已经使用过，看是否超额
                        if num < daily_quota:
                            # 今天没用满，当前 key 可用
                            return cur
                        # 超额了，继续往下找别的 key
                    else:
                        # lastuse 不是今天：视为新的一天，把 num 清零
                        self.df.at[idx, "num"] = 0
                        # 这里“检查阶段”不自增 num，只表示还可用
                        os.environ[env_name] = cur
                        return cur

            # ---------- 2. 从表里选一个今天还没用满的 key ----------
            for idx, row in self.df.iterrows():
                k = row.get("key")
                if not k:
                    continue

                last = row.get("lastuse")
                num_val = row.get("num")
                num = int(num_val) if pd.notna(num_val) else 0

                if pd.notna(last) and last.date() == today:
                    # 今天的使用次数，检查是否小于配额
                    if num >= daily_quota:
                        continue  # 该 key 已用满
                else:
                    # 不是今天：重置为 0
                    self.df.at[idx, "num"] = 0
                    num = 0

                # 找到一个今天还有额度的 key
                os.environ[env_name] = k
                return k

            # ---------- 3. 所有 key 都超额 ----------
            return None

        # 用锁保护整个“选择 key”过程
        return self._with_lock(do)


    def load_keys_from_txt(self, txt_path: Path):
        """
        读取 key.txt，每一行一个 key，调用 ExcelKeyTable.add 加入表中。
        """
        if not txt_path.exists():
            print(f"[warn] key 文件不存在: {txt_path}")
            return

        with txt_path.open("r", encoding="utf-8") as f:
            for line in f:
                key = line.strip()
                if not key:
                    continue
                self.add(key)


if __name__ == "__main__":
    KEY_TXT = Path("data/key.txt")
    tabel=ExcelKeyTable()
    tabel.load_keys_from_txt(KEY_TXT)