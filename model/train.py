from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402
from trainner import MultiTrainer  # noqa: E402
from alldatasets.loader import load_dataset  # noqa: E402


def _load_model_class():
    spec = importlib.util.spec_from_file_location(
        "apr_model",
        MODEL_DIR / "model.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Model


Model = _load_model_class()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="多智能体 reward 训练：input_trigger + naivesolver + solver"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="codecontestplus",
        choices=["codecontestplus", "apps"],
        help="训练数据集类型",
    )
    parser.add_argument(
        "--dataset_path",
        type=str,
        default="",
        help="数据集路径；为空时使用各数据集默认路径",
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
    )
    parser.add_argument(
        "--devices",
        type=str,
        default="cuda:0,cuda:1,cuda:2",
        help="三个 Agent 的设备；若设置了 CUDA_VISIBLE_DEVICES，请用 cuda:0,1,2",
    )
    parser.add_argument("--output_dir", type=str, default="outputs/solver_rl")
    parser.add_argument("--naive_bestofn", type=int, default=3)
    parser.add_argument("--solver_bestofn", type=int, default=3)
    parser.add_argument("--input_count", type=int, default=10)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--lr", type=float, default=1e-5)
    parser.add_argument(
        "--min_reward",
        type=float,
        default=0.0,
        help="低于该 reward 的 solver 样本跳过梯度更新",
    )
    parser.add_argument("--save_every", type=int, default=50)
    parser.add_argument("--log_file", type=str, default="")
    parser.add_argument(
        "--use_lora",
        action="store_true",
        help="仅对 solver 启用 LoRA 训练（推荐，显存占用更小）",
    )
    parser.add_argument("--lora_r", type=int, default=8)
    parser.add_argument("--lora_alpha", type=int, default=16)
    parser.add_argument("--lora_dropout", type=float, default=0.05)
    parser.add_argument(
        "--gradient_checkpointing",
        action="store_true",
        help="solver 训练时启用梯度检查点（进一步省显存）",
    )
    parser.add_argument(
        "--inject_mode",
        type=str,
        default="half",
        choices=[
            "none", "half", "fixed", "random", "geom",
            "tri", "gauss", "edge", "pow",
        ],
        help="执行超时时的 random 注入退避策略（见 injector.py）",
    )
    parser.add_argument(
        "--inject_value",
        type=float,
        default=10,
        help="注入退避强度（half 模式下用于缩小上界）",
    )
    parser.add_argument(
        "--exec_timeout",
        type=int,
        default=10,
        help="单次子进程执行超时（秒）",
    )
    parser.add_argument(
        "--no_inject_backoff",
        action="store_true",
        help="关闭注入退避，超时直接失败",
    )
    return parser.parse_args()


def validate_devices(devices: tuple[str, ...]) -> None:
    try:
        import torch
    except ImportError as e:
        raise RuntimeError("需要安装 torch") from e

    if not torch.cuda.is_available():
        raise RuntimeError("未检测到 CUDA，无法训练")

    n = torch.cuda.device_count()
    names = [torch.cuda.get_device_name(i) for i in range(n)]
    logging.info("可见 GPU 数量: %d → %s", n, names)

    for dev in devices:
        if not dev.startswith("cuda:"):
            raise ValueError(f"非法设备名 {dev!r}，请使用 cuda:N")
        idx = int(dev.split(":")[1])
        if idx < 0 or idx >= n:
            raise ValueError(
                f"设备 {dev} 不可用（当前仅有 cuda:0 .. cuda:{n - 1}）。"
                "若使用了 CUDA_VISIBLE_DEVICES，请改为 cuda:0,cuda:1,cuda:2"
            )


def setup_logging(log_file: str) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


def _exec_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "inject_mode": args.inject_mode,
        "inject_value": args.inject_value,
        "timeout": args.exec_timeout,
        "inject_backoff": not args.no_inject_backoff,
    }


def one_step(
    model: Any,
    trainer: MultiTrainer,
    question: str,
    *,
    naive_bestofn: int,
    solver_bestofn: int,
    input_count: int,
    min_reward: float,
    exec_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """单题训练一步：采样 -> 打分 -> 更新 solver。"""
    candidates = model.generate_candidates(
        naive_bestofn,
        solver_bestofn,
        question,
        input_count=input_count,
    )
    inputs = candidates.get("inputs") or []
    if not inputs:
        return {"skipped": True, "reason": "no_inputs"}

    matrices = trainer.build_matrices(candidates, exec_kwargs=exec_kwargs or {})
    if not matrices or not matrices[0]:
        return {"skipped": True, "reason": "empty_matrices"}

    rewards = trainer.calc_solver_rewards(matrices)
    prompt = model.solver.build_prompt(question)

    losses: List[float] = []
    updated = 0
    for code, reward in zip(candidates["solver_codes"], rewards):
        if reward <= min_reward:
            continue
        code = utils.clean_code(code)
        if not code.strip():
            continue
        loss = trainer.update_agent(
            model.solver,
            prompt,
            code,
            reward,
        )
        losses.append(loss)
        updated += 1

    return {
        "skipped": False,
        "num_inputs": len(inputs),
        "rewards": rewards,
        "losses": losses,
        "updated": updated,
    }


def save_checkpoint(model: Any, output_dir: Path, tag: str) -> None:
    ckpt_dir = output_dir / tag
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    model.solver.save(str(ckpt_dir))
    logging.info("checkpoint 已保存: %s", ckpt_dir)


def append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def resolve_dataset_path(args: argparse.Namespace) -> str:
    if args.dataset_path:
        return args.dataset_path
    if args.dataset == "apps":
        return "~/get_codeforces_data/APPS/train"
    return "~/lzh/datasets/codecontestplus"


def train_loop(
    model: Any,
    dataset: Any,
    trainer: MultiTrainer,
    args: argparse.Namespace,
) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "train_log.jsonl"

    end = args.end if args.end is not None else len(dataset.df)
    global_step = 0

    for epoch in range(args.epochs):
        logging.info("epoch %d/%d", epoch + 1, args.epochs)
        for idx in range(args.start, end):
            question = dataset.get_by_tag("description", idx)
            problem_id = dataset.get_by_tag("id", idx)

            stats = one_step(
                model,
                trainer,
                question,
                naive_bestofn=args.naive_bestofn,
                solver_bestofn=args.solver_bestofn,
                input_count=args.input_count,
                min_reward=args.min_reward,
                exec_kwargs=_exec_kwargs(args),
            )
            global_step += 1

            record = {
                "step": global_step,
                "epoch": epoch,
                "idx": idx,
                "id": str(problem_id),
                **stats,
            }
            append_jsonl(log_path, record)

            if stats.get("skipped"):
                logging.info(
                    "step=%d idx=%d id=%s skipped (%s)",
                    global_step,
                    idx,
                    problem_id,
                    stats.get("reason"),
                )
            else:
                logging.info(
                    "step=%d idx=%d id=%s rewards=%s losses=%s updated=%d",
                    global_step,
                    idx,
                    problem_id,
                    [round(r, 4) for r in stats["rewards"]],
                    [round(l, 6) for l in stats["losses"]],
                    stats["updated"],
                )

            if args.save_every > 0 and global_step % args.save_every == 0:
                save_checkpoint(model, output_dir, f"step_{global_step}")

    save_checkpoint(model, output_dir, "final")
    logging.info("训练完成，日志: %s", log_path)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)

    devices = tuple(d.strip() for d in args.devices.split(","))
    if len(devices) != 3:
        raise ValueError("--devices 需要 3 个设备，例如 cuda:0,cuda:1,cuda:2")
    validate_devices(devices)

    dataset_path = resolve_dataset_path(args)
    dataset = load_dataset(args.dataset, dataset_path)
    logging.info(
        "数据集加载完成 [%s] path=%s 共 %d 题",
        args.dataset,
        dataset_path,
        len(dataset.df),
    )

    logging.info(
        "训练模式: %s",
        "LoRA (仅 solver)" if args.use_lora else "全参数 (仅 solver，显存需求大)",
    )

    exec_kwargs = _exec_kwargs(args)
    logging.info(
        "代码执行: timeout=%ss inject_mode=%s backoff=%s",
        exec_kwargs["timeout"],
        exec_kwargs["inject_mode"],
        exec_kwargs["inject_backoff"],
    )

    model = Model(
        dataset,
        model_path=args.model_path,
        devices=devices,
        lr=args.lr,
        use_lora=args.use_lora,
        lora_r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        gradient_checkpointing=args.gradient_checkpointing,
        exec_kwargs=exec_kwargs,
    )
    trainer = MultiTrainer()
    train_loop(model, dataset, trainer, args)


if __name__ == "__main__":
    main()
