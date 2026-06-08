from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402
from trainner import MultiTrainer  # noqa: E402
from alldatasets.loader import load_dataset  # noqa: E402

try:
    from tqdm import tqdm  # noqa: E402
except ImportError:  # pragma: no cover
    tqdm = None  # type: ignore


def _load_model_class():
    spec = importlib.util.spec_from_file_location(
        "apr_model",
        MODEL_DIR / "model.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Model


Model = _load_model_class()

# Qwen2.5-Coder-7B 等：attention heads=28，vLLM tp 必须整除
DEFAULT_NUM_ATTENTION_HEADS = 28


def resolve_vllm_tp(
    requested: int,
    num_gpus: int,
    *,
    num_heads: int = DEFAULT_NUM_ATTENTION_HEADS,
) -> int:
    """选取不超过 requested 且能整除 num_heads 的最大 tp。"""
    if num_gpus < 2:
        return 1
    cap = min(max(1, requested), num_gpus - 1)
    for tp in range(cap, 0, -1):
        if num_heads % tp == 0:
            if tp != requested:
                logging.warning(
                    "vllm_tp_size=%d 不合法（%d 个头不能整除），已改为 %d。"
                    "可选: %s",
                    requested,
                    num_heads,
                    tp,
                    [x for x in (1, 2, 4, 7, 14, 28) if x <= cap and num_heads % x == 0],
                )
            return tp
    raise ValueError(
        f"无法为 {num_heads} 个 attention head 选择 vLLM tp（可见 GPU={num_gpus}）"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="多智能体 reward 训练：input_trigger + naivesolver + solver"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="codecontestplus",
        choices=["codecontestplus", "apps", "codecontests"],
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
        default="",
        help="hf: cuda:0,1,2；vllm: solver 训练卡，默认 cuda:6（7 卡 1-7 时 vLLM 占 cuda:0..5）",
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
        "--debug",
        action="store_true",
        help="打印全部日志；默认仅每步一行 step/rewards/losses",
    )
    parser.add_argument(
        "--use_lora",
        action="store_true",
        help="仅对 solver 启用 LoRA 训练（推荐，显存占用更小）",
    )
    parser.add_argument("--lora_r", type=int, default=64)
    parser.add_argument("--lora_alpha", type=int, default=128)
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
    parser.add_argument(
        "--exec_workers",
        type=int,
        default=8,
        help="run_solve 并行进程数（仅 CPU 子进程打分矩阵；1=串行）",
    )
    parser.add_argument(
        "--chat_backend",
        type=str,
        default="vllm",
        choices=["hf", "vllm"],
        help="默认 vllm：单实例批量 chat；hf=三卡 HuggingFace",
    )
    parser.add_argument(
        "--vllm_tp_size",
        type=int,
        default=4,
        help="vLLM tensor parallel（须整除 28：1/2/4/7；7 卡推荐 4）",
    )
    parser.add_argument(
        "--vllm_gpu_memory_utilization",
        type=float,
        default=0.85,
        help="vLLM 显存占用比例",
    )
    parser.add_argument(
        "--max_new_tokens",
        type=int,
        default=1024,
        help="chat 最大生成 token",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.7,
        help="chat 采样温度",
    )
    parser.add_argument(
        "--input_source",
        type=str,
        default="input_output",
        choices=["trigger", "input_output", "auto"],
        help="测例来源：input_output=APPS 官方 inputs；trigger=模型生成；auto=先 io 后 trigger",
    )
    parser.add_argument(
        "--val_size",
        type=int,
        default=300,
        help="从 APPS 抽取验证集题数（需有 input_output）；0=关闭验证",
    )
    parser.add_argument(
        "--val_every",
        type=int,
        default=25,
        help="每 N 次参数更新后验证；训练前另跑 update_step=0 基线",
    )
    parser.add_argument(
        "--val_seed",
        type=int,
        default=42,
        help="验证集抽样随机种子（可复现）",
    )
    parser.add_argument(
        "--val_indices_file",
        type=str,
        default="",
        help="验证集 idx 列表 JSON；存在则加载，否则抽样并写入 output_dir/val_indices.json",
    )
    parser.add_argument(
        "--val_input_count",
        type=int,
        default=0,
        help="验证时每题测例数；0=与 --input_count 相同",
    )
    return parser.parse_args()


def parse_devices_arg(devices_str: str, chat_backend: str) -> tuple[str, ...]:
    devices = tuple(d.strip() for d in devices_str.split(",") if d.strip())
    if chat_backend == "vllm":
        if not devices:
            devices = ("cuda:6",)
        return devices
    if not devices:
        devices = ("cuda:0", "cuda:1", "cuda:2")
    if len(devices) != 3:
        raise ValueError("--devices 需要 3 个设备，例如 cuda:0,cuda:1,cuda:2")
    return devices


def validate_devices(devices: tuple[str, ...], chat_backend: str = "hf") -> None:
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


def validate_vllm_layout(
    devices: tuple[str, ...],
    vllm_tp_size: int,
) -> int:
    import torch

    n = torch.cuda.device_count()
    tp = resolve_vllm_tp(vllm_tp_size, n)
    solver_idx = int(devices[-1].split(":")[1])
    if solver_idx < tp:
        raise ValueError(
            f"vLLM 占用 cuda:0..cuda:{tp - 1}，solver 训练卡 cuda:{solver_idx} 与之冲突。"
            f"请设 --devices cuda:{tp} 或更大（7 卡常用 --vllm_tp_size 4 --devices cuda:6）"
        )
    return tp


TRAIN_SUMMARY_LOGGER = "apr.train"


class _TrainSummaryOnlyFilter(logging.Filter):
    """非 debug 时只放行 apr.train 的 step 摘要行。"""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name == TRAIN_SUMMARY_LOGGER


def setup_logging(log_file: str, *, debug: bool = False) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    if not debug:
        stream.addFilter(_TrainSummaryOnlyFilter())
    stream.setLevel(logging.INFO)
    root.addHandler(stream)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        if not debug:
            fh.addFilter(_TrainSummaryOnlyFilter())
        fh.setLevel(logging.INFO)
        root.addHandler(fh)

    logging.getLogger(TRAIN_SUMMARY_LOGGER).setLevel(logging.INFO)


def _exec_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "inject_mode": args.inject_mode,
        "inject_value": args.inject_value,
        "timeout": args.exec_timeout,
        "inject_backoff": not args.no_inject_backoff,
        "exec_workers": max(1, args.exec_workers),
    }


def one_step(
    model: Any,
    trainer: MultiTrainer,
    question: str,
    *,
    idx: int,
    global_step: int = 0,
    naive_bestofn: int,
    solver_bestofn: int,
    input_count: int,
    min_reward: float,
    exec_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """单题训练一步：rollout（vLLM+LoRA / HF+LoRA）-> 打分 -> HF 更新 LoRA。"""
    lora_synced = False
    cache = getattr(model, "rollout_lora_cache_dir", None)
    if (
        cache
        and getattr(model, "chat_backend", "") == "vllm"
        and getattr(model, "_use_lora", False)
    ):
        model.sync_solver_lora_for_vllm(
            cache,
            lora_int_id=max(1, int(global_step)),
        )
        lora_synced = True

    try:
        candidates = model.generate_candidates(
            naive_bestofn,
            solver_bestofn,
            question,
            input_count=input_count,
            idx=idx,
            use_trainable_solver=True,
        )
        solver_gen = model.ensure_solver_not_base(context="训练 rollout")
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
            "input_source": candidates.get("input_source"),
            "solver_gen_backend": solver_gen,
            "rewards": rewards,
            "losses": losses,
            "updated": updated,
        }
    finally:
        if lora_synced:
            model.clear_solver_lora_snapshot()


def eval_one(
    model: Any,
    trainer: MultiTrainer,
    question: str,
    *,
    idx: int,
    naive_bestofn: int,
    solver_bestofn: int,
    input_count: int,
    exec_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """单题验证：只打分，不更新参数；solver 用 LoRA（验证时 vLLM+LoRA 快照）。"""
    candidates = model.generate_candidates(
        naive_bestofn,
        solver_bestofn,
        question,
        input_count=input_count,
        idx=idx,
        use_trainable_solver=True,
    )
    solver_gen = model.ensure_solver_not_base(context="验证")
    inputs = candidates.get("inputs") or []
    if not inputs:
        return {"skipped": True, "reason": "no_inputs", "mean_reward": 0.0}

    matrices = trainer.build_matrices(candidates, exec_kwargs=exec_kwargs or {})
    if not matrices or not matrices[0]:
        return {"skipped": True, "reason": "empty_matrices", "mean_reward": 0.0}

    rewards = trainer.calc_solver_rewards(matrices)
    mean_r = sum(rewards) / len(rewards) if rewards else 0.0

    run_kw = exec_kwargs or {}
    expected = _load_gt_outputs(model, idx, len(inputs))
    exp_slice = expected[: len(inputs)] if expected else []

    bestofn_pass = False
    if exp_slice and len(exp_slice) >= len(inputs):
        bestofn_pass = utils.solver_pass_at_1(
            candidates["solver_codes"],
            inputs,
            exp_slice,
            **run_kw,
        )

    pass_at_1 = False
    if exp_slice and len(exp_slice) >= len(inputs):
        fresh_codes = model.generate_solver_codes(
            question, n=1, use_trainable_solver=True
        )
        model.ensure_solver_not_base(context="验证 pass@1")
        if fresh_codes:
            pass_at_1 = utils.solver_passes_all_cases(
                fresh_codes[0],
                inputs,
                exp_slice,
                **run_kw,
            )

    return {
        "skipped": False,
        "num_inputs": len(inputs),
        "solver_gen_backend": solver_gen,
        "rewards": rewards,
        "mean_reward": mean_r,
        "max_reward": max(rewards) if rewards else 0.0,
        "bestofn_pass": bestofn_pass,
        "pass_at_1": pass_at_1,
    }


def _load_gt_outputs(model: Any, idx: int, max_count: int) -> List[str]:
    getter_out = getattr(model.dataset, "get_io_outputs", None)
    if not callable(getter_out):
        return []
    try:
        return list(getter_out(idx, max_count=max_count))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return []


def run_validation(
    model: Any,
    trainer: MultiTrainer,
    dataset: Any,
    val_indices: List[int],
    *,
    global_step: int,
    update_step: int,
    args: argparse.Namespace,
    output_dir: Path,
) -> Dict[str, Any]:
    """在固定验证集上评估当前 solver（无梯度；vLLM+LoRA 或 HF LoRA 生成）。"""
    if not val_indices:
        return {"skipped": True, "reason": "no_val_indices"}

    was_training = model.solver.model.training
    model.solver.model.eval()

    val_lora_dir = output_dir / "_val_lora_snapshot"
    use_vllm_lora = (
        getattr(model, "chat_backend", "") == "vllm"
        and getattr(model, "_use_lora", False)
    )
    if use_vllm_lora:
        model.sync_solver_lora_for_vllm(
            str(val_lora_dir),
            lora_int_id=max(1, int(update_step)),
        )
        logging.info(
            "验证使用 vLLM+LoRA snapshot: %s (lora_int_id=%d)",
            val_lora_dir,
            update_step,
        )
    else:
        model.clear_solver_lora_snapshot()

    input_count = args.val_input_count or args.input_count
    exec_kwargs = _exec_kwargs(args)
    per_problem: List[Dict[str, Any]] = []
    reward_sum = 0.0
    ok_n = 0
    skip_n = 0
    pass_n = 0
    bestofn_pass_n = 0

    iter_val = val_indices
    if tqdm is not None:
        iter_val = tqdm(
            val_indices,
            desc=f"val update_step={update_step} train_step={global_step}",
            unit="题",
            dynamic_ncols=True,
            file=sys.stderr,
        )

    try:
        for v_idx in iter_val:
            question = dataset.get_by_tag("description", v_idx)
            st = eval_one(
                model,
                trainer,
                question,
                idx=v_idx,
                naive_bestofn=args.naive_bestofn,
                solver_bestofn=args.solver_bestofn,
                input_count=input_count,
                exec_kwargs=exec_kwargs,
            )
            per_problem.append(
                {
                    "idx": v_idx,
                    "id": str(dataset.get_by_tag("id", v_idx)),
                    "skipped": st.get("skipped", False),
                    "reason": st.get("reason", ""),
                    "mean_reward": round(float(st.get("mean_reward", 0.0)), 4),
                    "max_reward": round(float(st.get("max_reward", 0.0)), 4),
                    "bestofn_pass": bool(st.get("bestofn_pass", False)),
                    "pass_at_1": bool(st.get("pass_at_1", False)),
                }
            )
            if st.get("skipped"):
                skip_n += 1
            else:
                ok_n += 1
                reward_sum += float(st.get("mean_reward", 0.0))
                if st.get("pass_at_1"):
                    pass_n += 1
                if st.get("bestofn_pass"):
                    bestofn_pass_n += 1

            if tqdm is not None and hasattr(iter_val, "set_postfix"):
                iter_val.set_postfix(
                    ok=ok_n,
                    skip=skip_n,
                    p1=pass_n,
                    bn=bestofn_pass_n,
                    refresh=False,
                )
    finally:
        if tqdm is not None and hasattr(iter_val, "close"):
            iter_val.close()
        model.clear_solver_lora_snapshot()
        if was_training:
            model.solver.model.train()

    mean_reward = reward_sum / ok_n if ok_n else 0.0
    pass_at_1_rate = pass_n / ok_n if ok_n else 0.0
    bestofn_pass_rate = bestofn_pass_n / ok_n if ok_n else 0.0
    summary = {
        "train_step": global_step,
        "update_step": update_step,
        "n_val": len(val_indices),
        "n_ok": ok_n,
        "n_skip": skip_n,
        "n_pass_at_1": pass_n,
        "n_bestofn_pass": bestofn_pass_n,
        "mean_reward": mean_reward,
        "pass_at_1": pass_at_1_rate,
        "bestofn_pass_rate": bestofn_pass_rate,
        "problems": per_problem,
    }
    append_jsonl(output_dir / "val_log.jsonl", summary)
    return summary


def save_checkpoint(model: Any, output_dir: Path, tag: str) -> None:
    ckpt_dir = output_dir / tag
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    model.solver.save(str(ckpt_dir))
    logging.info("checkpoint 已保存: %s", ckpt_dir)


def append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _indices_with_io(dataset: Any) -> List[int]:
    """有 input_output 测例的题目 idx。"""
    out: List[int] = []
    n = len(dataset.df)
    for idx in range(n):
        try:
            ins = dataset.get_io_inputs(idx, max_count=1)
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            ins = []
        if ins:
            out.append(idx)
    return out


def load_or_build_val_indices(
    dataset: Any,
    output_dir: Path,
    *,
    val_size: int,
    val_seed: int,
    val_indices_file: str,
) -> List[int]:
    if val_size <= 0:
        return []

    path = Path(val_indices_file).expanduser() if val_indices_file else None
    if path and path.is_file():
        data = json.loads(path.read_text(encoding="utf-8"))
        indices = data.get("indices") if isinstance(data, dict) else data
        if not isinstance(indices, list):
            raise ValueError(f"无效的验证集文件: {path}")
        return [int(i) for i in indices]

    pool = _indices_with_io(dataset)
    if len(pool) < val_size:
        raise ValueError(
            f"仅有 {len(pool)} 题含 input_output，少于 val_size={val_size}"
        )
    rng = random.Random(val_seed)
    chosen = sorted(rng.sample(pool, val_size))

    out_path = output_dir / "val_indices.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(
            {
                "dataset_size": len(dataset.df),
                "pool_size": len(pool),
                "val_size": val_size,
                "seed": val_seed,
                "indices": chosen,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return chosen


def resolve_dataset_path(args: argparse.Namespace) -> str:
    if args.dataset_path:
        return args.dataset_path
    if args.dataset == "apps":
        return "~/get_codeforces_data/APPS/train"
    if args.dataset == "codecontests":
        base = Path("~/datasets/codecontests").expanduser()
        if args.dataset_path:
            base = Path(args.dataset_path).expanduser()
        extracted = base / "extracted_tasks"
        if extracted.is_dir() and any(extracted.glob("code_contests-*")):
            return str(extracted)
        return str(base)
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
    model.rollout_lora_cache_dir = str(output_dir / "_rollout_lora_snapshot")

    end = args.end if args.end is not None else len(dataset.df)
    global_step = 0
    update_step = 0
    last_val_update_step = -1

    summary = logging.getLogger(TRAIN_SUMMARY_LOGGER)

    val_indices = load_or_build_val_indices(
        dataset,
        output_dir,
        val_size=args.val_size,
        val_seed=args.val_seed,
        val_indices_file=args.val_indices_file,
    )
    val_set: Set[int] = set(val_indices)
    train_indices = [i for i in range(args.start, end) if i not in val_set]
    if val_set:
        summary.info(
            "val_init n=%d train_n=%d val_indices=%s",
            len(val_indices),
            len(train_indices),
            str(output_dir / "val_indices.json"),
        )

    if val_indices and args.val_every > 0:
        val_stats = run_validation(
            model,
            trainer,
            dataset,
            val_indices,
            global_step=0,
            update_step=0,
            args=args,
            output_dir=output_dir,
        )
        summary.info(
            "val update_step=0 train_step=0 n=%d ok=%d skip=%d "
            "pass@1=%.4f (%d/%d) bestofn_pass=%.4f (%d/%d) mean_reward=%.4f",
            val_stats.get("n_val", 0),
            val_stats.get("n_ok", 0),
            val_stats.get("n_skip", 0),
            float(val_stats.get("pass_at_1", 0.0)),
            val_stats.get("n_pass_at_1", 0),
            val_stats.get("n_ok", 0),
            float(val_stats.get("bestofn_pass_rate", 0.0)),
            val_stats.get("n_bestofn_pass", 0),
            val_stats.get("n_ok", 0),
            float(val_stats.get("mean_reward", 0.0)),
        )
        last_val_update_step = 0

    for epoch in range(args.epochs):
        if args.debug:
            logging.info("epoch %d/%d", epoch + 1, args.epochs)
        for idx in train_indices:
            question = dataset.get_by_tag("description", idx)
            problem_id = dataset.get_by_tag("id", idx)

            stats = one_step(
                model,
                trainer,
                question,
                idx=idx,
                global_step=global_step,
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
            did_update = False
            if not stats.get("skipped") and int(stats.get("updated") or 0) > 0:
                update_step += 1
                record["update_step"] = update_step
                did_update = True
            append_jsonl(log_path, record)

            if stats.get("skipped"):
                if args.debug:
                    logging.info(
                        "step=%d idx=%d id=%s skipped (%s)",
                        global_step,
                        idx,
                        problem_id,
                        stats.get("reason"),
                    )
            else:
                summary.info(
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

            if (
                val_indices
                and args.val_every > 0
                and did_update
                and update_step % args.val_every == 0
            ):
                val_stats = run_validation(
                    model,
                    trainer,
                    dataset,
                    val_indices,
                    global_step=global_step, 
                    update_step=update_step,
                    args=args,
                    output_dir=output_dir,
                )
                summary.info(
                    "val update_step=%d train_step=%d n=%d ok=%d skip=%d "
                    "pass@1=%.4f (%d/%d) bestofn_pass=%.4f (%d/%d) mean_reward=%.4f",
                    update_step,
                    global_step,
                    val_stats.get("n_val", 0),
                    val_stats.get("n_ok", 0),
                    val_stats.get("n_skip", 0),
                    float(val_stats.get("pass_at_1", 0.0)),
                    val_stats.get("n_pass_at_1", 0),
                    val_stats.get("n_ok", 0),
                    float(val_stats.get("bestofn_pass_rate", 0.0)),
                    val_stats.get("n_bestofn_pass", 0),
                    val_stats.get("n_ok", 0),
                    float(val_stats.get("mean_reward", 0.0)),
                )
                last_val_update_step = update_step

    save_checkpoint(model, output_dir, "final")
    if (
        val_indices
        and args.val_every > 0
        and update_step > 0
        and update_step != last_val_update_step
    ):
        val_stats = run_validation(
            model,
            trainer,
            dataset,
            val_indices,
            global_step=global_step,
            update_step=update_step,
            args=args,
            output_dir=output_dir,
        )
        summary.info(
            "val update_step=%d (final) train_step=%d n=%d ok=%d skip=%d "
            "pass@1=%.4f (%d/%d) bestofn_pass=%.4f (%d/%d) mean_reward=%.4f",
            update_step,
            global_step,
            val_stats.get("n_val", 0),
            val_stats.get("n_ok", 0),
            val_stats.get("n_skip", 0),
            float(val_stats.get("pass_at_1", 0.0)),
            val_stats.get("n_pass_at_1", 0),
            val_stats.get("n_ok", 0),
            float(val_stats.get("bestofn_pass_rate", 0.0)),
            val_stats.get("n_bestofn_pass", 0),
            val_stats.get("n_ok", 0),
            float(val_stats.get("mean_reward", 0.0)),
        )
    if args.debug:
        logging.info("训练完成，日志: %s", log_path)


def validate_train_lora_policy(args: argparse.Namespace) -> None:
    """vLLM 训练必须用 LoRA，避免 solver rollout/验证误用冻结基座。"""
    if args.chat_backend == "vllm" and not args.use_lora:
        raise ValueError(
            "chat_backend=vllm 时必须 --use_lora。"
            "否则 vLLM 仅服务 naive/trigger，solver 若误走 vLLM 基座会原地踏步。"
        )


def main() -> None:
    args = parse_args()
    validate_train_lora_policy(args)
    setup_logging(args.log_file, debug=args.debug)

    devices = parse_devices_arg(args.devices, args.chat_backend)
    validate_devices(devices, args.chat_backend)

    vllm_tp = args.vllm_tp_size
    if args.chat_backend == "vllm":
        vllm_tp = validate_vllm_layout(devices, args.vllm_tp_size)

    dataset_path = resolve_dataset_path(args)
    dataset = load_dataset(args.dataset, dataset_path)

    if args.debug:
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
            "代码执行: timeout=%ss inject_mode=%s backoff=%s workers=%s",
            exec_kwargs["timeout"],
            exec_kwargs["inject_mode"],
            exec_kwargs["inject_backoff"],
            exec_kwargs["exec_workers"],
        )
        logging.info(
            "chat: backend=%s vllm_tp=%s max_new_tokens=%s input_source=%s",
            args.chat_backend,
            vllm_tp,
            args.max_new_tokens,
            args.input_source,
        )
    else:
        exec_kwargs = _exec_kwargs(args)

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
        chat_backend=args.chat_backend,
        vllm_tp_size=vllm_tp,
        vllm_gpu_memory_utilization=args.vllm_gpu_memory_utilization,
        max_new_tokens=args.max_new_tokens,
        temperature=args.temperature,
        input_source=args.input_source,
    )
    trainer = MultiTrainer()
    summary = logging.getLogger(TRAIN_SUMMARY_LOGGER)
    if args.chat_backend == "vllm":
        summary.info(
            "solver 路径: rollout/val=vLLM+LoRA快照 | update=HF LoRA | "
            "vLLM基座仅 naive/trigger"
        )
    else:
        summary.info(
            "solver 路径: rollout/val/update 均 HF+%s",
            "LoRA" if args.use_lora else "全参",
        )
    train_loop(model, dataset, trainer, args)


if __name__ == "__main__":
    main()
