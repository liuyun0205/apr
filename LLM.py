from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class LLMConfig:
    """
    model_type:
      - "local": 本地 Transformers 模型（AutoModelForCausalLM）
      - "api":   外部 API（优先用 openai SDK；否则走 OpenAI 兼容 HTTP）
    """

    model_type: str  # "local" | "api"
    model: str
    system_prompt: str = ""

    # API config（OpenAI/兼容接口）
    base_url: Optional[str] = None
    api_key: Optional[str] = None

    # generation params
    max_new_tokens: int = 512
    temperature: float = 0.7

    # DeepSpeed ZeRO (local only)
    # 说明：本仓库当前只做推理；ZeRO 在训练中更有意义。
    # 这里提供一个“可选启用”的 DeepSpeed 包装，便于后续扩展到训练/分布式。
    use_zero: bool = False
    zero_stage: int = 3  # 1|2|3
    zero_offload: str = "none"  # "none" | "cpu"


class LLM:
    def __init__(self, config: LLMConfig):
        self.config = config
        self._backend: str = config.model_type.lower().strip()

        if self._backend not in {"local", "api"}:
            raise ValueError(f"Unsupported model_type: {config.model_type!r} (expected 'local' or 'api')")

        self._client = None
        self._tokenizer = None
        self._model = None

        if self._backend == "local":
            self._init_local()
        else:
            self._init_api()

    def _init_local(self) -> None:
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "本地模型模式需要安装 transformers（以及 torch）。"
            ) from e

        self._tokenizer = AutoTokenizer.from_pretrained(self.config.model)
        self._model = AutoModelForCausalLM.from_pretrained(
            self.config.model,
            device_map="auto",
        )

        if self.config.use_zero:
            # DeepSpeed ZeRO：当前代码以推理为主，这里仅做“可选接入”
            # - 若你后续要训练：建议新增训练脚本并用 deepspeed.initialize + zero_optimization 配置
            try:
                import deepspeed  # type: ignore
            except Exception as e:  # pragma: no cover
                raise RuntimeError(
                    "已启用 use_zero，但未安装 deepspeed。请先 `pip install deepspeed`。"
                ) from e

            # 注意：推理场景下 DeepSpeed 更常用 init_inference（不是严格意义的 ZeRO 训练）
            # 这里不改变 generate API：把模型包装成 inference engine，并保留 .generate 调用。
            mp_size = int(os.getenv("WORLD_SIZE", "1"))
            engine = deepspeed.init_inference(
                self._model,
                mp_size=mp_size,
                dtype=getattr(self._model, "dtype", None),
                replace_method="auto",
                replace_with_kernel_inject=True,
            )
            self._model = engine.module

    def _init_api(self) -> None:
        # 允许通过环境变量配置（不强制用户提交 .env）
        base_url = self.config.base_url or os.getenv("OPENAI_BASE_URL")
        api_key = self.config.api_key or os.getenv("OPENAI_API_KEY")

        # 优先使用 openai SDK（它也支持 OpenAI-compatible base_url）
        try:
            from openai import OpenAI  # type: ignore

            kwargs: Dict[str, Any] = {}
            if base_url:
                kwargs["base_url"] = base_url
            if api_key:
                kwargs["api_key"] = api_key

            self._client = OpenAI(**kwargs)
            return
        except Exception:
            # SDK 不可用则 fallback 到 HTTP（OpenAI 兼容：/v1/chat/completions）
            self._client = {"base_url": base_url, "api_key": api_key}

    def chat(self, user_content: str, *, system_prompt: Optional[str] = None) -> str:
        sys_prompt = self.config.system_prompt if system_prompt is None else system_prompt

        if self._backend == "local":
            return self._chat_local(user_content, sys_prompt)
        return self._chat_api(user_content, sys_prompt)

    def _chat_local(self, user_content: str, system_prompt: str) -> str:
        assert self._tokenizer is not None and self._model is not None

        # 简单拼接（兼容纯 CausalLM）；如需 chat template 可在此扩展
        prompt = (system_prompt.strip() + "\n\n" if system_prompt.strip() else "") + user_content
        inputs = self._tokenizer(prompt, return_tensors="pt").to(self._model.device)

        outputs = self._model.generate(
            **inputs,
            max_new_tokens=self.config.max_new_tokens,
            do_sample=True,
            temperature=self.config.temperature,
        )

        text = self._tokenizer.decode(outputs[0], skip_special_tokens=True)
        return text

    def _chat_api(self, user_content: str, system_prompt: str) -> str:
        messages: List[Dict[str, str]] = []
        if system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_content})

        # openai SDK path
        if hasattr(self._client, "chat"):
            resp = self._client.chat.completions.create(  # type: ignore[attr-defined]
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
            )
            return resp.choices[0].message.content

        # HTTP fallback path (OpenAI-compatible)
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("外部 API 模式需要 openai SDK 或 requests。") from e

        base_url = (self._client or {}).get("base_url") or "https://api.openai.com"
        api_key = (self._client or {}).get("api_key")
        url = base_url.rstrip("/") + "/v1/chat/completions"

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        payload: Dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
        }

        r = requests.post(url, headers=headers, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]

