"""
随机数范围注入退避：执行超时则缩小 random.* 上界后重试。
逻辑来自 get_codeforces_data/injector.py
"""
from __future__ import annotations

import logging
import math
import os
import random
import re
import subprocess
import sys
import tempfile
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

EXIT_TIMEOUT = 255


def _clamp(x, lo, hi):
    return max(lo, min(x, hi))


def _weighted_pick(cands, weights):
    s = sum(weights)
    r = random.random() * s
    acc = 0.0
    for c, w in zip(cands, weights):
        acc += w
        if r <= acc:
            return c
    return cands[-1]


class Injector:
    @staticmethod
    def injection(mode: str, content: str, value=1) -> Tuple[str, bool]:
        pattern = r"(random\.\w+\()\s*(.+?)\s*,\s*(.+?)\s*(\))"
        changed = False

        def is_const_expr(s: str) -> bool:
            return re.search(r"[A-Za-z_]", s) is None

        def transform(m):
            nonlocal changed
            str_a = m.group(2).strip()
            str_b = m.group(3).strip()
            a_is_const = is_const_expr(str_a)
            b_is_const = is_const_expr(str_b)

            if a_is_const and b_is_const:
                try:
                    a_val = int(eval(str_a))
                    b_val = int(eval(str_b))
                except Exception:
                    return m.group(0)

                if mode == "fixed":
                    toa = a_val
                    tob = int(b_val * value)
                elif mode == "half":
                    toa = a_val
                    max_tob = 10**20
                    tob = max(a_val, min((b_val // value) + 1, max_tob))
                elif mode == "random":
                    toa = a_val
                    tob = random.randint(a_val, b_val)
                elif mode == "none":
                    return m.group(0)
                elif mode == "geom":
                    toa = a_val
                    span = b_val - a_val
                    if span <= 0:
                        tob = b_val
                    else:
                        strength = max(1.0, float(value))
                        p = 1.0 / strength
                        p = _clamp(p, 1e-6, 0.999999)
                        k = int(math.log(1 - random.random()) / math.log(1 - p))
                        tob = a_val + _clamp(k, 0, span)
                    tob = _clamp(tob, a_val, b_val)
                elif mode == "tri":
                    toa = a_val
                    peak = a_val + (b_val - a_val) * 0.2
                    tob = int(random.triangular(a_val, b_val, peak))
                    tob = _clamp(tob, a_val, b_val)
                elif mode == "gauss":
                    toa = a_val
                    span = max(1, b_val - a_val)
                    sigma = max(1.0, span / max(1.0, float(value)))
                    mu = a_val + span * 0.2
                    tob = int(random.gauss(mu, sigma))
                    tob = _clamp(tob, a_val, b_val)
                elif mode == "edge":
                    toa = a_val
                    cands = [a_val, a_val + 1, b_val - 1, b_val]
                    cands = [x for x in cands if a_val <= x <= b_val]
                    tob = _weighted_pick(cands, [3, 1, 1, 3][: len(cands)])
                    tob = _clamp(tob, a_val, b_val)
                elif mode == "pow":
                    toa = a_val
                    span = max(0, b_val - a_val)
                    alpha = max(1e-6, float(value))
                    u = random.random()
                    tob = a_val + int(round((u**alpha) * span))
                    tob = _clamp(tob, a_val, b_val)
                else:
                    return m.group(0)

                if toa == a_val and tob == b_val:
                    return m.group(0)

                changed = True
                return f"{m.group(1)}{toa}, {tob}{m.group(4)}"

            if (not a_is_const) and b_is_const:
                try:
                    b_val = int(eval(str_b))
                except Exception:
                    return m.group(0)

                if mode == "fixed":
                    tob = int(b_val * value)
                elif mode == "half":
                    max_tob = 10**20
                    tob = min(int(b_val // value) + 1, max_tob)
                elif mode == "random":
                    tob = random.randint(0, b_val)
                elif mode == "none":
                    return m.group(0)
                elif mode == "geom":
                    span = b_val
                    if span <= 0:
                        tob = b_val
                    else:
                        strength = max(1.0, float(value))
                        p = 1.0 / strength
                        p = _clamp(p, 1e-6, 0.999999)
                        k = int(math.log(1 - random.random()) / math.log(1 - p))
                        tob = _clamp(k, 0, span)
                    tob = _clamp(tob, 0, b_val)
                elif mode == "tri":
                    peak = b_val * 0.2
                    tob = int(random.triangular(0, b_val, peak))
                    tob = _clamp(tob, 0, b_val)
                elif mode == "gauss":
                    span = max(1, b_val)
                    sigma = max(1.0, span / max(1.0, float(value)))
                    mu = span * 0.2
                    tob = int(random.gauss(mu, sigma))
                    tob = _clamp(tob, 0, b_val)
                elif mode == "edge":
                    cands = [0, 1, b_val - 1, b_val]
                    cands = [x for x in cands if 0 <= x <= b_val]
                    tob = _weighted_pick(cands, [3, 1, 1, 3][: len(cands)])
                    tob = _clamp(tob, 0, b_val)
                elif mode == "pow":
                    alpha = max(1e-6, float(value))
                    u = random.random()
                    tob = int(round((u**alpha) * b_val))
                    tob = _clamp(tob, 0, b_val)
                else:
                    return m.group(0)

                if tob == b_val:
                    return m.group(0)

                changed = True
                return f"{m.group(1)}{str_a}, {tob}{m.group(4)}"

            return m.group(0)

        modified_content, _n = re.subn(pattern, transform, content)
        return modified_content, changed

    @staticmethod
    def run_file(file_path: str, timeout: int = 10) -> Tuple[str, int]:
        try:
            result = subprocess.run(
                [sys.executable, file_path],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=timeout,
            )
            return result.stdout.strip(), result.returncode
        except subprocess.TimeoutExpired:
            return "Timeout", EXIT_TIMEOUT

    @staticmethod
    def run_with_backoff(
        content: str,
        *,
        mode: str = "half",
        value: float = 10,
        timeout: int = 10,
        max_rounds: int = 32,
        enabled: bool = True,
    ) -> Tuple[str, int]:
        """
        执行 Python 源码；超时则 injection 缩小 random 范围后重试。
        返回 (stdout, exit_code)，255 表示最终仍超时。
        """
        if not enabled or mode == "none":
            path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".py",
                    delete=False,
                    encoding="utf-8",
                ) as f:
                    f.write(content)
                    path = f.name
                return Injector.run_file(path, timeout=timeout)
            finally:
                if path:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        current = content
        last_stdout = "Timeout"
        last_code = EXIT_TIMEOUT

        for round_idx in range(max_rounds):
            path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".py",
                    delete=False,
                    encoding="utf-8",
                ) as f:
                    f.write(current)
                    path = f.name

                stdout, code = Injector.run_file(path, timeout=timeout)
                last_stdout, last_code = stdout, code
                if code != EXIT_TIMEOUT:
                    return stdout, code

                logger.info(
                    "执行超时，注入退避 round=%d mode=%s value=%s",
                    round_idx + 1,
                    mode,
                    value,
                )
                current, ok = Injector.injection(mode, current, value=value)
                if not ok:
                    logger.warning("无法继续 injection（无 random 范围可缩小）")
                    return last_stdout, last_code
            finally:
                if path:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

        return last_stdout, last_code
