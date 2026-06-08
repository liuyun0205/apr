#!/usr/bin/env python3
"""从训练 output_dir/val_log.jsonl 绘制验证曲线（pass@1、BoN pass@1、mean reward）。"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

FIG_PASS_AT_1 = "val_fig_pass_at_1.png"
FIG_BON_PASS_AT_1 = "val_fig_bon_pass_at_1.png"
FIG_MEAN_REWARD = "val_fig_mean_reward.png"


def load_val_log(path: Path) -> List[Dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"未找到验证日志: {path}")

    records: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"{path}:{line_no} JSON 解析失败: {e}") from e
    return records


def extract_series(
    records: List[Dict[str, Any]],
    x_key: str,
) -> Tuple[List[int], List[float], List[float], List[float]]:
    xs: List[int] = []
    pass_at_1: List[float] = []
    bon: List[float] = []
    rewards: List[float] = []

    for rec in records:
        if rec.get("skipped"):
            continue
        if x_key not in rec:
            continue
        xs.append(int(rec[x_key]))
        pass_at_1.append(float(rec.get("pass_at_1", 0.0)))
        bon.append(float(rec.get("bestofn_pass_rate", 0.0)))
        rewards.append(float(rec.get("mean_reward", 0.0)))

    order = sorted(range(len(xs)), key=lambda i: xs[i])
    xs = [xs[i] for i in order]
    pass_at_1 = [pass_at_1[i] for i in order]
    bon = [bon[i] for i in order]
    rewards = [rewards[i] for i in order]
    return xs, pass_at_1, bon, rewards


def _plot_single(
    xs: Sequence[int],
    ys: Sequence[float],
    *,
    ylabel: str,
    out_path: Path,
    title: str,
    ylim: Optional[Tuple[float, float]] = None,
    color: str = "#2563eb",
    marker: str = "o",
) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError as e:
        raise RuntimeError(
            "绘图需要 matplotlib：pip install matplotlib"
        ) from e

    fig, ax = plt.subplots(figsize=(8, 4), constrained_layout=True)
    ax.plot(xs, ys, marker=marker, linewidth=1.5, markersize=4, color=color)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("step")
    if ylim is not None:
        ax.set_ylim(*ylim)
    ax.grid(True, alpha=0.3)
    ax.set_title(title)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def save_validation_figures(
    output_dir: Path,
    *,
    val_log: Optional[Path] = None,
    x_axis: str = "train_step",
    title_prefix: str = "",
) -> List[Path]:
    """
    读取 val_log.jsonl，保存三幅验证曲线到 output_dir：
      - val_fig_pass_at_1.png
      - val_fig_bon_pass_at_1.png
      - val_fig_mean_reward.png
    """
    output_dir = Path(output_dir).expanduser().resolve()
    log_path = val_log or (output_dir / "val_log.jsonl")
    records = load_val_log(log_path)
    xs, pass_at_1, bon, rewards = extract_series(records, x_axis)
    if not xs:
        raise ValueError(f"val_log 中无可用记录（需字段 {x_axis!r}）: {log_path}")

    prefix = f"{title_prefix} — " if title_prefix else ""
    rate_ylim = (-0.05, 1.05)

    specs = [
        (pass_at_1, "pass@1", FIG_PASS_AT_1, rate_ylim, "#16a34a", "o"),
        (bon, "BoN pass@1", FIG_BON_PASS_AT_1, rate_ylim, "#2563eb", "o"),
        (rewards, "Mean reward", FIG_MEAN_REWARD, None, "#dc2626", "s"),
    ]
    saved: List[Path] = []
    for ys, ylabel, filename, ylim, color, marker in specs:
        out = output_dir / filename
        _plot_single(
            xs,
            ys,
            ylabel=ylabel,
            out_path=out,
            title=f"{prefix}{ylabel}",
            ylim=ylim,
            color=color,
            marker=marker,
        )
        saved.append(out)
    return saved


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="绘制训练验证曲线：pass@1、BoN pass@1、mean reward（横轴 steps）",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=".",
        help="训练输出目录（默认当前目录，需含 val_log.jsonl）",
    )
    parser.add_argument(
        "--val-log",
        default="",
        help="val_log.jsonl 路径（默认 <output_dir>/val_log.jsonl）",
    )
    parser.add_argument(
        "--x-axis",
        choices=("train_step", "update_step"),
        default="train_step",
        help="横轴字段：train_step=全局步数，update_step=参数更新次数",
    )
    parser.add_argument(
        "--title",
        default="",
        help="图标题前缀（默认用 output_dir 名）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    val_log = (
        Path(args.val_log).expanduser()
        if args.val_log
        else None
    )
    title_prefix = args.title or output_dir.name

    try:
        saved = save_validation_figures(
            output_dir,
            val_log=val_log,
            x_axis=args.x_axis,
            title_prefix=title_prefix,
        )
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 1
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    records = load_val_log(val_log or (output_dir / "val_log.jsonl"))
    xs, pass_at_1, bon, rewards = extract_series(records, args.x_axis)

    for path in saved:
        print(f"已保存: {path}")
    print(f"  点数: {len(xs)}")
    print(f"  横轴: {args.x_axis} [{xs[0]} .. {xs[-1]}]")
    print(f"  pass@1: {pass_at_1[-1]:.4f} (末次)")
    print(f"  BoN pass@1: {bon[-1]:.4f} (末次)")
    print(f"  mean reward: {rewards[-1]:.4f} (末次)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
