#!/usr/bin/env python3
"""
Solver SFT 冷启动：用 CodeContests+ accepted solution 监督微调 solver LoRA。

与 RL 阶段 prompt 格式一致（prompt/solver.txt + chat template），
仅对 completion（代码）计算 loss。

示例：
  # 1) 准备数据
  python model/prepare_solver_sft.py \\
    --dataset_path ~/datasets/codecontestplus \\
    --output outputs/solver_sft/train.jsonl

  # 2) SFT 冷启动
  CUDA_VISIBLE_DEVICES=6 python model/sft_solver.py \\
    --model_path ~/lzh/Qwen2.5-Coder-7B-Instruct \\
    --train_jsonl outputs/solver_sft/train.jsonl \\
    --output_dir outputs/solver_sft/lora \\
    --use_lora \\
    --gradient_checkpointing

  # 3) RL 训练加载冷启动权重
  python model/train.py ... --solver_lora_init outputs/solver_sft/lora/final

  # 4) 评测 SFT LoRA（免合并，vLLM 直接挂 adapter）
  python alldatasets/eval.py \\
    --dataset cure_codecontests \\
    --dataset-path ~/datasets/CURE_codecontests \\
    --split test \\
    --model-type local --model ~/lzh/Qwen2.5-Coder-7B-Instruct \\
    --lora outputs/solver_sft/lora/final \\
    --solver-bestofn 16 --gpu 1 --resume
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import torch
from torch.utils.data import DataLoader, Dataset

MODEL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODEL_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODEL_DIR))
os.chdir(PROJECT_ROOT)

import utils  # noqa: E402
from agent import Agent  # noqa: E402

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None  # type: ignore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Solver SFT 冷启动（CodeContests+ accepted solutions）")
    p.add_argument("--model_path", type=str, required=True)
    p.add_argument("--train_jsonl", type=str, required=True)
    p.add_argument("--output_dir", type=str, default="outputs/solver_sft/lora")
    p.add_argument("--device", type=str, default="cuda:0")
    p.add_argument("--epochs", type=int, default=1)
    p.add_argument("--lr", type=float, default=2e-5)
    p.add_argument("--batch_size", type=int, default=1)
    p.add_argument("--gradient_accumulation_steps", type=int, default=8)
    p.add_argument("--max_length", type=int, default=4096)
    p.add_argument("--warmup_ratio", type=float, default=0.03)
    p.add_argument("--weight_decay", type=float, default=0.0)
    p.add_argument("--max_grad_norm", type=float, default=1.0)
    p.add_argument("--save_every", type=int, default=500)
    p.add_argument("--log_every", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--use_lora", action="store_true", help="LoRA 微调（推荐，与 RL 阶段兼容）")
    p.add_argument("--lora_r", type=int, default=64)
    p.add_argument("--lora_alpha", type=int, default=128)
    p.add_argument("--lora_dropout", type=float, default=0.05)
    p.add_argument(
        "--lora_init",
        type=str,
        default="",
        help="从已有 LoRA 继续 SFT（目录含 adapter_config.json）",
    )
    p.add_argument("--gradient_checkpointing", action="store_true")
    p.add_argument(
        "--prompt_file",
        type=str,
        default="prompt/solver.txt",
        help="solver system prompt，须与 RL 阶段一致",
    )
    p.add_argument("--start", type=int, default=0, help="jsonl 行偏移（断点续训）")
    p.add_argument("--max_steps", type=int, default=0, help="0=跑完所有 epoch")
    return p.parse_args()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_jsonl_records(path: str, *, start: int = 0) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with Path(path).expanduser().open(encoding="utf-8") as f:
        for line_no, line in enumerate(f):
            if line_no < start:
                continue
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            question = str(obj.get("question") or "").strip()
            code = utils.clean_code(str(obj.get("code") or ""))
            if not question or not code.strip():
                continue
            records.append(
                {
                    "idx": obj.get("idx"),
                    "problem_id": obj.get("problem_id", ""),
                    "question": question,
                    "code": code,
                    "language": obj.get("language", ""),
                }
            )
    if not records:
        raise ValueError(f"无有效训练样本: {path} (start={start})")
    return records


class SolverSFTDataset(Dataset):
    def __init__(self, records: List[Dict[str, Any]], agent: Agent):
        self.records = records
        self.agent = agent

    def __len__(self) -> int:
        return len(self.records)

    def __getitem__(self, i: int) -> Dict[str, str]:
        rec = self.records[i]
        prompt = self.agent.build_prompt(rec["question"])
        return {
            "prompt": prompt,
            "completion": rec["code"],
            "problem_id": str(rec.get("problem_id", "")),
        }


def collate_batch(
    batch: List[Dict[str, str]],
    *,
    tokenizer,
    max_length: int,
) -> Dict[str, torch.Tensor]:
    input_ids_list: List[torch.Tensor] = []
    labels_list: List[torch.Tensor] = []
    attn_list: List[torch.Tensor] = []

    for item in batch:
        prompt = item["prompt"]
        completion = item["completion"]
        full_text = prompt + completion

        prompt_ids = tokenizer(
            prompt,
            add_special_tokens=False,
            truncation=False,
        )["input_ids"]
        full_enc = tokenizer(
            full_text,
            add_special_tokens=False,
            truncation=True,
            max_length=max_length,
        )
        input_ids = full_enc["input_ids"]
        if not input_ids:
            continue

        labels = list(input_ids)
        prompt_len = min(len(prompt_ids), len(labels))
        for j in range(prompt_len):
            labels[j] = -100

        input_ids_list.append(torch.tensor(input_ids, dtype=torch.long))
        labels_list.append(torch.tensor(labels, dtype=torch.long))
        attn_list.append(torch.ones(len(input_ids), dtype=torch.long))

    if not input_ids_list:
        raise ValueError("batch 为空（可能 max_length 过小）")

    max_len = max(t.size(0) for t in input_ids_list)
    pad_id = tokenizer.pad_token_id
    if pad_id is None:
        pad_id = tokenizer.eos_token_id

    def _pad(seqs: List[torch.Tensor], pad_value: int) -> torch.Tensor:
        out = torch.full((len(seqs), max_len), pad_value, dtype=torch.long)
        for i, seq in enumerate(seqs):
            out[i, : seq.size(0)] = seq
        return out

    return {
        "input_ids": _pad(input_ids_list, pad_id),
        "attention_mask": _pad(attn_list, 0),
        "labels": _pad(labels_list, -100),
    }


def save_checkpoint(agent: Agent, output_dir: Path, tag: str) -> None:
    ckpt = output_dir / tag
    ckpt.mkdir(parents=True, exist_ok=True)
    agent.save(str(ckpt))
    logging.info("checkpoint 已保存: %s", ckpt)


def train_loop(agent: Agent, records: List[Dict[str, Any]], args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "sft_log.jsonl"

    dataset = SolverSFTDataset(records, agent)
    loader = DataLoader(
        dataset,
        batch_size=max(1, args.batch_size),
        shuffle=True,
        collate_fn=lambda batch: collate_batch(
            batch,
            tokenizer=agent.tokenizer,
            max_length=args.max_length,
        ),
    )

    optimizer = agent.optimizer
    if optimizer is None:
        raise RuntimeError("Agent 未启用训练（trainable=False）")

    total_steps_per_epoch = (len(dataset) + args.batch_size - 1) // args.batch_size
    total_steps = total_steps_per_epoch * args.epochs
    if args.max_steps > 0:
        total_steps = min(total_steps, args.max_steps)

    warmup_steps = max(1, int(total_steps * args.warmup_ratio))
    global_step = 0
    micro_step = 0
    running_loss = 0.0
    optimizer.zero_grad()

    agent.model.train()
    for epoch in range(args.epochs):
        epoch_iter = loader
        if tqdm is not None:
            epoch_iter = tqdm(
                loader,
                desc=f"SFT epoch {epoch + 1}/{args.epochs}",
                unit="batch",
            )

        for batch in epoch_iter:
            if args.max_steps > 0 and global_step >= args.max_steps:
                break

            device = agent.device
            batch = {k: v.to(device) for k, v in batch.items()}
            outputs = agent.model(**batch)
            loss = outputs.loss / max(1, args.gradient_accumulation_steps)
            loss.backward()
            micro_step += 1
            running_loss += float(outputs.loss.item())

            if micro_step % args.gradient_accumulation_steps == 0:
                torch.nn.utils.clip_grad_norm_(
                    agent.model.parameters(),
                    max_norm=args.max_grad_norm,
                )
                lr_scale = min(1.0, (global_step + 1) / warmup_steps)
                for pg in optimizer.param_groups:
                    pg["lr"] = args.lr * lr_scale
                optimizer.step()
                optimizer.zero_grad()
                global_step += 1

                if global_step % args.log_every == 0:
                    avg = running_loss / args.log_every
                    logging.info(
                        "step=%d epoch=%d loss=%.4f lr=%.2e",
                        global_step,
                        epoch + 1,
                        avg,
                        optimizer.param_groups[0]["lr"],
                    )
                    record = {
                        "step": global_step,
                        "epoch": epoch,
                        "loss": round(avg, 6),
                        "lr": optimizer.param_groups[0]["lr"],
                    }
                    with log_path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    running_loss = 0.0

                if args.save_every > 0 and global_step % args.save_every == 0:
                    save_checkpoint(agent, output_dir, f"step_{global_step}")

                if tqdm is not None and hasattr(epoch_iter, "set_postfix"):
                    epoch_iter.set_postfix(step=global_step, refresh=False)

        if args.max_steps > 0 and global_step >= args.max_steps:
            break

    save_checkpoint(agent, output_dir, "final")
    meta = {
        "model_path": args.model_path,
        "train_jsonl": args.train_jsonl,
        "num_records": len(records),
        "epochs": args.epochs,
        "global_steps": global_step,
        "use_lora": args.use_lora,
        "lora_r": args.lora_r,
        "lora_alpha": args.lora_alpha,
        "prompt_file": args.prompt_file,
    }
    (output_dir / "sft_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logging.info("SFT 完成，共 %d 步 → %s", global_step, output_dir / "final")


def main() -> None:
    args = parse_args()
    setup_logging()
    random.seed(args.seed)
    torch.manual_seed(args.seed)

    system_prompt = utils.file2text(args.prompt_file)
    lora_init = (args.lora_init or "").strip()

    agent = Agent(
        model_path=args.model_path,
        system_prompt=system_prompt,
        device=args.device,
        lr=args.lr,
        trainable=True,
        use_lora=args.use_lora or bool(lora_init),
        lora_r=args.lora_r,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        gradient_checkpointing=args.gradient_checkpointing,
        lora_path=lora_init,
    )

    records = load_jsonl_records(args.train_jsonl, start=args.start)
    logging.info(
        "加载 %d 条 SFT 样本，model=%s lora=%s",
        len(records),
        args.model_path,
        lora_init or ("new" if args.use_lora else "full"),
    )
    train_loop(agent, records, args)


if __name__ == "__main__":
    main()
