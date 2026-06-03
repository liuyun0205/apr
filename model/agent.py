import logging

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# Qwen / Llama 类模型常用的 LoRA 注入层
DEFAULT_LORA_TARGET_MODULES = (
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
)


class Agent:

    def __init__(
        self,
        model_path,
        system_prompt,
        device,
        lr=1e-5,
        *,
        use_lora: bool = False,
        trainable: bool = False,
        lora_r: int = 8,
        lora_alpha: int = 16,
        lora_dropout: float = 0.05,
        lora_target_modules=None,
        gradient_checkpointing: bool = False,
    ):
        self.device = device
        self.system_prompt = system_prompt
        self.use_lora = use_lora
        self.trainable = trainable

        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            trust_remote_code=True,
        )

        self.model = AutoModelForCausalLM.from_pretrained(
            model_path,
            trust_remote_code=True,
            dtype=torch.bfloat16,
        ).to(device)

        if use_lora:
            self._apply_lora(
                r=lora_r,
                alpha=lora_alpha,
                dropout=lora_dropout,
                target_modules=lora_target_modules,
            )

        if gradient_checkpointing and trainable:
            self.model.gradient_checkpointing_enable()
            if hasattr(self.model, "enable_input_require_grads"):
                self.model.enable_input_require_grads()

        if not trainable:
            self.model.eval()
            for p in self.model.parameters():
                p.requires_grad = False
            self.optimizer = None
        else:
            trainable_params = [p for p in self.model.parameters() if p.requires_grad]
            if not trainable_params:
                raise RuntimeError("trainable=True 但没有可训练参数")
            self.optimizer = torch.optim.AdamW(trainable_params, lr=lr)
            n_trainable = sum(p.numel() for p in trainable_params)
            n_total = sum(p.numel() for p in self.model.parameters())
            logging.info(
                "Agent 可训练参数: %s / %s (%.4f%%)",
                f"{n_trainable:,}",
                f"{n_total:,}",
                100.0 * n_trainable / max(n_total, 1),
            )

    def _apply_lora(self, *, r, alpha, dropout, target_modules):
        try:
            from peft import LoraConfig, get_peft_model
        except ImportError as e:
            raise ImportError(
                "LoRA 需要安装 peft: pip install peft"
            ) from e

        if target_modules is None:
            target_modules = list(DEFAULT_LORA_TARGET_MODULES)

        config = LoraConfig(
            r=r,
            lora_alpha=alpha,
            lora_dropout=dropout,
            target_modules=target_modules,
            bias="none",
            task_type="CAUSAL_LM",
        )
        self.model = get_peft_model(self.model, config)
        self.model.print_trainable_parameters()

    def build_prompt(self, question: str) -> str:
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": question},
        ]
        return self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )

    @torch.no_grad()
    def chat(self, prompt, max_new_tokens=1024, temperature=0.7):
        self.model.eval()

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": prompt},
        ]

        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )

        inputs = self.tokenizer(
            text,
            return_tensors="pt",
        ).to(self.device)

        outputs = self.model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=True,
            temperature=temperature,
            pad_token_id=self.tokenizer.eos_token_id,
        )

        answer = self.tokenizer.decode(
            outputs[0][inputs.input_ids.shape[1]:],
            skip_special_tokens=True,
        )

        if self.trainable:
            self.model.train()

        return answer

    def save(self, path):
        self.model.save_pretrained(path)
        self.tokenizer.save_pretrained(path)
