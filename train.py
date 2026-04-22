from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional


def _read_jsonl_texts(path: str, *, text_key: str = "text") -> List[str]:
    texts: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if text_key not in obj:
                raise ValueError(f"jsonl 每行必须包含 key={text_key!r}，但缺失：{obj.keys()}")
            texts.append(str(obj[text_key]))
    if not texts:
        raise ValueError(f"数据为空：{path}")
    return texts


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CausalLM SFT training with DeepSpeed ZeRO")
    p.add_argument("--model", type=str, required=True, help="HF model name or local path")
    p.add_argument("--train_jsonl", type=str, required=True, help="jsonl file, each line: {\"text\": ...}")
    p.add_argument("--output_dir", type=str, default="outputs")

    p.add_argument("--max_length", type=int, default=1024)
    p.add_argument("--learning_rate", type=float, default=2e-5)
    p.add_argument("--num_train_epochs", type=int, default=1)
    p.add_argument("--per_device_train_batch_size", type=int, default=1)
    p.add_argument("--gradient_accumulation_steps", type=int, default=1)
    p.add_argument("--warmup_ratio", type=float, default=0.03)
    p.add_argument("--weight_decay", type=float, default=0.0)
    p.add_argument("--logging_steps", type=int, default=10)
    p.add_argument("--save_steps", type=int, default=200)
    p.add_argument("--save_total_limit", type=int, default=2)

    p.add_argument(
        "--deepspeed",
        type=str,
        default="config/ds_config_zero3.json",
        help="DeepSpeed config json path (e.g. config/ds_config_zero2.json / config/ds_config_zero3.json)",
    )
    p.add_argument("--no_deepspeed", action="store_true", help="Disable DeepSpeed (for debug/single GPU)")

    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    try:
        import torch  # type: ignore
        from transformers import (  # type: ignore
            AutoModelForCausalLM,
            AutoTokenizer,
            DataCollatorForLanguageModeling,
            Trainer,
            TrainingArguments,
        )
    except Exception as e:
        raise RuntimeError("训练需要安装 torch + transformers（以及 deepspeed 才能启用 ZeRO）。") from e

    # tokenizer / model
    tokenizer = AutoTokenizer.from_pretrained(args.model, use_fast=True)
    if tokenizer.pad_token is None:
        # 对 CausalLM 来说，常见做法是用 eos 作为 pad
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(args.model)
    model.config.use_cache = False  # 训练时避免 cache 占显存

    # dataset (最小实现：jsonl -> tokenized)
    texts = _read_jsonl_texts(args.train_jsonl, text_key="text")

    try:
        from datasets import Dataset  # type: ignore
    except Exception as e:
        raise RuntimeError("请安装 datasets：`pip install datasets`（用于最简单的数据加载）。") from e

    ds = Dataset.from_dict({"text": texts})

    def _tok(batch: Dict[str, List[str]]) -> Dict[str, List[List[int]]]:
        out = tokenizer(
            batch["text"],
            max_length=args.max_length,
            truncation=True,
            padding=False,
        )
        return out

    ds = ds.map(_tok, batched=True, remove_columns=["text"])

    data_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False)

    deepspeed_cfg: Optional[str] = None
    if not args.no_deepspeed:
        deepspeed_cfg = args.deepspeed
        if not os.path.exists(deepspeed_cfg):
            raise FileNotFoundError(f"找不到 deepspeed 配置文件：{deepspeed_cfg}")

    # TrainingArguments
    use_bf16 = torch.cuda.is_available() and torch.cuda.get_device_capability(0)[0] >= 8
    ta = TrainingArguments(
        output_dir=args.output_dir,
        per_device_train_batch_size=args.per_device_train_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        learning_rate=args.learning_rate,
        warmup_ratio=args.warmup_ratio,
        weight_decay=args.weight_decay,
        num_train_epochs=args.num_train_epochs,
        logging_steps=args.logging_steps,
        save_steps=args.save_steps,
        save_total_limit=args.save_total_limit,
        report_to=[],
        bf16=use_bf16,
        fp16=not use_bf16,
        deepspeed=deepspeed_cfg,
        seed=args.seed,
    )

    trainer = Trainer(
        model=model,
        args=ta,
        train_dataset=ds,
        data_collator=data_collator,
        tokenizer=tokenizer,
    )

    trainer.train()
    trainer.save_model()
    tokenizer.save_pretrained(args.output_dir)


if __name__ == "__main__":
    main()

