import csv
import os
from datetime import datetime


CSV_COLUMNS = ("key", "num", "lastuse")


class KeyRow:
    def __init__(self, key, num, lastuse):
        self.key = key
        self.num = num
        self.lastuse = lastuse


def nowIsoLocal():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(now.microsecond / 1000):03d}"


def todayYmd():
    return datetime.now().strftime("%Y-%m-%d")


def ymdFromLastuse(lastuse):
    # 只要能拿到 YYYY-mm-dd 前缀就行；解析失败则返回空，视作“很久没用”
    s = (lastuse or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return ""


def maskKey(k):
    if len(k) <= 10:
        return k[:2] + "***"
    return f"{k[:6]}...{k[-4:]}"


class Keyring:
    """
    用 key.csv 做一个简单的“按天计数 + 超限轮换”。

    约定：
    - num：当日已使用次数
    - lastuse：最后一次使用时间（本地时间字符串）
    - 若 lastuse 的日期不是今天：视为新的一天，num 在选择时会被重置为 0
    """

    def __init__(self, csv_path, *, daily_limit=190):
        self.csv_path = csv_path
        self.daily_limit = int(daily_limit)

    def acquire(self):
        """
        选择一个“当日未超限”的 key（不写回 csv、不增加计数）。

        计数写回请用 record_success()；遇到 quota/429 可用 mark_exhausted()。
        这样网络/TLS 抖动导致的重试不会把 key 的当日次数“烧光”。
        """
        header, raw_rows = self.readCsv()
        header = self.normalizeHeader(header)
        rows = self.parseRows(raw_rows)

        today = todayYmd()
        # 新的一天：对“非今日 lastuse”的 key 重置 num=0（只在内存中）
        for r in rows:
            if ymdFromLastuse(r.lastuse) != today:
                r.num = 0

        # 选择策略：优先 num 小，其次 lastuse 早（空视作早）
        def score(r):
            return (r.num, r.lastuse or "")

        candidates = [r for r in rows if r.num < self.daily_limit]
        if not candidates:
            raise RuntimeError("所有 key 今日用量已达上限，请明天再试或增加 key。")

        chosen = min(candidates, key=score)
        return chosen.key

    def record_success(self, key):
        """
        记录一次“成功使用”：num += 1, lastuse=now，并写回 csv。
        """
        header, raw_rows = self.readCsv()
        header = self.normalizeHeader(header)
        rows = self.parseRows(raw_rows)

        today = todayYmd()
        for r in rows:
            if r.key != key:
                continue
            if ymdFromLastuse(r.lastuse) != today:
                r.num = 0
            r.num += 1
            r.lastuse = nowIsoLocal()
            self.writeBack(header, raw_rows, rows)
            return

    def today_count(self, key):
        """返回 key 当日已记录使用次数（非今日 lastuse 视为 0）。"""
        _, raw_rows = self.readCsv()
        rows = self.parseRows(raw_rows)
        today = todayYmd()
        for r in rows:
            if r.key != key:
                continue
            if ymdFromLastuse(r.lastuse) != today:
                return 0
            return int(r.num)
        return 0

    def is_below_daily_limit(self, key):
        return self.today_count(key) < self.daily_limit

    def mark_exhausted(self, key):
        """
        将某个 key 标记为“今日耗尽”（num=limit，lastuse=now），用于遇到 quota/429 时快速切换。
        """
        header, raw_rows = self.readCsv()
        header = self.normalizeHeader(header)
        rows = self.parseRows(raw_rows)

        today = todayYmd()
        for r in rows:
            if r.key == key:
                if ymdFromLastuse(r.lastuse) != today:
                    r.num = 0
                r.num = max(r.num, self.daily_limit)
                r.lastuse = nowIsoLocal()
                self.writeBack(header, raw_rows, rows)
                return

    def readCsv(self):
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(self.csv_path)
        with open(self.csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            rows = list(reader)
        return header, rows

    def normalizeHeader(self, header):
        h = [x.strip() for x in header if x and x.strip()]
        for c in CSV_COLUMNS:
            if c not in h:
                h.append(c)
        rest = [x for x in h if x not in CSV_COLUMNS]
        return list(CSV_COLUMNS) + rest

    def parseRows(self, rows):
        parsed = []
        for r in rows:
            k = (r.get("key") or "").strip()
            if not k:
                continue
            n_raw = (r.get("num") or "0").strip()
            try:
                n = int(float(n_raw))
            except Exception:
                n = 0
            lu = (r.get("lastuse") or "").strip()
            parsed.append(KeyRow(key=k, num=n, lastuse=lu))
        if not parsed:
            raise ValueError("CSV 里没有可用的 key 行（需要列 key/num/lastuse）。")
        return parsed

    def writeBack(self, header, raw_rows, rows):
        # 只根据 key 匹配回写（若重复 key，只更新第一条匹配）
        by_key = {r.key: r for r in rows}
        for rr in raw_rows:
            k = (rr.get("key") or "").strip()
            if not k or k not in by_key:
                continue
            r = by_key[k]
            rr["num"] = str(r.num)
            rr["lastuse"] = r.lastuse

        tmp = self.csv_path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for rr in raw_rows:
                writer.writerow({k: rr.get(k, "") for k in header})
        os.replace(tmp, self.csv_path)


_now_iso_local = nowIsoLocal
_today_ymd = todayYmd
_ymd_from_lastuse = ymdFromLastuse
_mask_key = maskKey

__all__ = ["Keyring", "maskKey"]

