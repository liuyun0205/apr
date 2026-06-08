from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# 503/502 等网关临时错误：自动重试
RETRIABLE_HTTP_STATUS = frozenset({408, 429, 500, 502, 503, 504})

# newapi_channel_conn.url，POST 原样打到该地址，不拼路径
DEFAULT_API_BASE_URL = "https://api.chshapi.org/v1/chat/completions"


def resolve_api_base_url(*, cli_base_url: str = "") -> str:
    """解析 API 根地址，优先 CLI / CHSHAPI_*，默认 chshapi.org。"""
    explicit = (cli_base_url or "").strip()
    if explicit:
        return _normalize_api_base_url(explicit)
    env_chsh = (os.getenv("CHSHAPI_BASE_URL") or "").strip()
    if env_chsh:
        return _normalize_api_base_url(env_chsh)
    env_openai = (os.getenv("OPENAI_BASE_URL") or "").strip()
    if env_openai and "chshapi.org" in env_openai:
        return _normalize_api_base_url(env_openai)
    return DEFAULT_API_BASE_URL


def _normalize_api_base_url(base_url: str) -> str:
    return (base_url or "").strip().rstrip("/")


def _api_max_retries() -> int:
    return max(1, int(os.getenv("OPENAI_MAX_RETRIES", "6")))


def _api_retry_sleep(attempt: int) -> float:
    base = max(0.5, float(os.getenv("OPENAI_RETRY_SLEEP", "3")))
    return min(base * (2**attempt), 60.0)


def _http_status_from_exc(exc: Exception) -> Optional[int]:
    resp = getattr(exc, "response", None)
    if resp is not None:
        try:
            return int(resp.status_code)
        except Exception:
            pass
    sc = getattr(exc, "status_code", None)
    if sc is not None:
        try:
            return int(sc)
        except Exception:
            pass
    text = str(exc)
    for code in RETRIABLE_HTTP_STATUS:
        if str(code) in text:
            return code
    return None


def _is_retriable_api_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    try:
        import requests  # type: ignore

        if isinstance(
            exc,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ),
        ):
            return True
    except Exception:
        pass
    st = _http_status_from_exc(exc)
    return st in RETRIABLE_HTTP_STATUS if st is not None else False


def _format_api_error(exc: Exception, *, url: str = "") -> str:
    resp = getattr(exc, "response", None)
    if resp is not None:
        try:
            body = (resp.text or "").strip()[:800]
            return f"{exc!r} url={url} body={body!r}"
        except Exception:
            pass
    if url:
        return f"{exc!r} url={url}"
    return repr(exc)


@dataclass(frozen=True)
class LLMConfig:
    """
    model_type:
      - "local":  本地 vLLM
      - "api" / "direct": POST 到 base_url（OpenAI 兼容 JSON body）
    """

    model_type: str  # "local" | "api" | "direct"
    model: str
    model_path:str
    system_prompt: str = ""


    base_url: Optional[str] = None
    api_key: Optional[str] = None

    max_new_tokens: int = 512
    temperature: float = 0.7

    # vLLM（local 模式）；选卡用 CUDA_VISIBLE_DEVICES=0,1,2
    tensor_parallel_size: int = 1
    gpu_memory_utilization: float = 0.9
    max_model_len: Optional[int] = None
    enable_lora: bool = False
    max_lora_rank: int = 64

    use_zero: bool = False
    zero_stage: int = 3
    zero_offload: str = "none"


class LLM:
    def __init__(self, config: LLMConfig):
        self.config = config
        self._backend= config.model_type.lower().strip()

        if self._backend not in {"local", "api", "direct"}:
            raise ValueError(
                f"Unsupported model_type: {config.model_type!r} (expected 'local', 'api', or 'direct')"
            )
        if self._backend == "direct":
            self._backend = "api"

        self._client = None
        self._tokenizer = None
        self._model = None
        self._sampling_params_cls = None

        if self._backend == "local":
            self._init_local()
        else:
            self._init_api()

    def _init_local(self) -> None:
        if self.config.use_zero:
            raise RuntimeError(
                "local 模式已改用 vLLM，不再支持 --use_zero / DeepSpeed。"
                "多卡请设 --tensor-parallel-size 或 CUDA_VISIBLE_DEVICES。"
            )

        try:
            from vllm import LLM as VLLMEngine, SamplingParams  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "本地模型模式需要 vLLM（Python>=3.10）。"
                "示例：conda activate py311 && pip install vllm"
            ) from e

        # 关闭 vLLM 控制台 tqdm / 冗长 INFO（Processed prompts / Adding requests）
        os.environ.setdefault("VLLM_LOGGING_LEVEL", "ERROR")

        self._sampling_params_cls = SamplingParams
        vllm_kwargs: Dict[str, Any] = {
            "model": self.config.model,
            "tensor_parallel_size": max(1, int(self.config.tensor_parallel_size)),
            "trust_remote_code": True,
            "gpu_memory_utilization": float(self.config.gpu_memory_utilization),
        }
        if self.config.max_model_len is not None:
            vllm_kwargs["max_model_len"] = int(self.config.max_model_len)
        if self.config.enable_lora:
            vllm_kwargs["enable_lora"] = True
            vllm_kwargs["max_loras"] = 1
            vllm_kwargs["max_lora_rank"] = max(1, int(self.config.max_lora_rank))

        self._model = VLLMEngine(**vllm_kwargs)
        self._tokenizer = self._model.get_tokenizer()

    def _init_api(self) -> None:
        api_key = (
            self.config.api_key
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("CHSHAPI_API_KEY")
        )
        base_url = resolve_api_base_url(cli_base_url=self.config.base_url or "")
        # 只用 base_url 发 HTTP，不用 OpenAI SDK（SDK 会自行拼 /chat/completions 等路径）
        self._client = None
        self._client_meta = {"base_url": base_url, "api_key": api_key}

    def chat(self, user_content: str, *, system_prompt: Optional[str] = None) -> str:
        return self.chat_batch([user_content], system_prompt=system_prompt)[0]

    def chat_batch(
        self,
        user_contents: List[str],
        *,
        system_prompt: Optional[str] = None,
        lora_path: Optional[str] = None,
        lora_int_id: int = 1,
        lora_name: str = "solver",
    ) -> List[str]:
        if not user_contents:
            return []
        sys_prompt = self.config.system_prompt if system_prompt is None else system_prompt

        if self._backend == "local":
            return self._chat_batch_local(
                user_contents,
                sys_prompt,
                lora_path=lora_path,
                lora_int_id=lora_int_id,
                lora_name=lora_name,
            )
        return [self._chat_api(u, sys_prompt) for u in user_contents]

    def _build_messages(self, user_content: str, system_prompt: str) -> List[Dict[str, str]]:
        messages: List[Dict[str, str]] = []
        if system_prompt.strip():
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_content})
        return messages

    def _messages_to_prompt(self, messages: List[Dict[str, str]]) -> str:
        parts = [(msg.get("content") or "").strip() for msg in messages]
        return "\n\n".join(p for p in parts if p)

    def _encode_messages_for_local(self, messages: List[Dict[str, str]]) -> str:
        assert self._tokenizer is not None
        apply_tpl = getattr(self._tokenizer, "apply_chat_template", None)
        if callable(apply_tpl):
            try:
                return apply_tpl(messages, tokenize=False, add_generation_prompt=True)
            except Exception:
                pass
        return self._messages_to_prompt(messages)

    def _local_sampling_params(self) -> Any:
        assert self._sampling_params_cls is not None
        return self._sampling_params_cls(
            temperature=self.config.temperature,
            max_tokens=self.config.max_new_tokens,
        )

    @staticmethod
    def _vllm_output_text(outputs: Any) -> str:
        if isinstance(outputs, str):
            return outputs
        if not outputs:
            return ""
        item = outputs[0] if isinstance(outputs, list) else outputs
        if isinstance(item, str):
            return item
        out_list = getattr(item, "outputs", None)
        if out_list:
            return str(out_list[0].text)
        return str(getattr(item, "text", item))

    def _chat_local(self, user_content: str, system_prompt: str) -> str:
        return self._chat_batch_local([user_content], system_prompt)[0]

    def _vllm_generate_batch(
        self,
        prompts: List[str],
        sampling: Any,
        *,
        lora_request: Any = None,
    ) -> List[Any]:
        """批量 generate，并尽量关闭 tqdm。"""
        gen_kw: Dict[str, Any] = dict(
            sampling_params=sampling,
            use_tqdm=False,
        )
        if lora_request is not None:
            gen_kw["lora_request"] = lora_request
        try:
            return self._model.generate(prompts, **gen_kw)
        except TypeError:
            gen_kw.pop("use_tqdm", None)
            return self._model.generate(prompts, **gen_kw)

    def _build_lora_request(
        self,
        *,
        lora_path: Optional[str],
        lora_int_id: int,
        lora_name: str,
    ) -> Any:
        if not lora_path:
            return None
        if not self.config.enable_lora:
            raise RuntimeError(
                "vLLM 未启用 LoRA（enable_lora=False），无法加载 adapter。"
                "训练时请传 --use_lora。"
            )
        from vllm.lora.request import LoRARequest  # type: ignore

        path = str(lora_path).strip()
        if not path:
            return None
        return LoRARequest(
            lora_name=lora_name,
            lora_int_id=max(1, int(lora_int_id)),
            lora_path=path,
        )

    def _chat_batch_local(
        self,
        user_contents: List[str],
        system_prompt: str,
        *,
        lora_path: Optional[str] = None,
        lora_int_id: int = 1,
        lora_name: str = "solver",
    ) -> List[str]:
        assert self._model is not None

        sampling = self._local_sampling_params()
        messages_list = [
            self._build_messages(u, system_prompt) for u in user_contents
        ]
        prompts = [
            self._encode_messages_for_local(m) for m in messages_list
        ]
        lora_request = self._build_lora_request(
            lora_path=lora_path,
            lora_int_id=lora_int_id,
            lora_name=lora_name,
        )
        outputs = self._vllm_generate_batch(
            prompts,
            sampling,
            lora_request=lora_request,
        )
        return [self._vllm_output_text([o]) for o in outputs]

    def _chat_api(self, user_content: str, system_prompt: str) -> str:
        messages = self._build_messages(user_content, system_prompt)
        max_retries = _api_max_retries()
        last_err: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                return self._chat_api_once(messages)
            except Exception as e:
                last_err = e
                if not _is_retriable_api_error(e) or attempt >= max_retries - 1:
                    raise RuntimeError(_format_api_error(e)) from e
                delay = _api_retry_sleep(attempt)
                try:
                    from tqdm import tqdm  # type: ignore

                    st = _http_status_from_exc(e)
                    tqdm.write(
                        f"[api] retry {attempt + 1}/{max_retries - 1} "
                        f"after {st or 'error'}, sleep {delay:.1f}s"
                    )
                except Exception:
                    pass
                time.sleep(delay)

        raise RuntimeError(_format_api_error(last_err or RuntimeError("api failed")))

    def _chat_api_once(self, messages: List[Dict[str, str]]) -> str:
        return self._chat_api_http(messages)

    def _chat_api_http(self, messages: List[Dict[str, str]]) -> str:
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("外部 API 模式需要 requests：`pip install requests`") from e

        meta = getattr(self, "_client_meta", None) or {}
        url = _normalize_api_base_url(meta.get("base_url") or DEFAULT_API_BASE_URL)

        payload: Dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
        }

        headers = {"Content-Type": "application/json"}
        api_key = meta.get("api_key")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code >= 400:
            body = (r.text or "").strip()[:800]
            err = RuntimeError(f"HTTP {r.status_code} url={url} body={body!r}")
            err.response = r  # type: ignore[attr-defined]
            raise err
        data = r.json()
        return data["choices"][0]["message"]["content"]

    def release(self) -> None:
        """释放 vLLM / 本地模型占用的 GPU 显存。"""
        import gc

        if self._backend == "local" and self._model is not None:
            engine = self._model
            self._model = None
            self._tokenizer = None
            self._sampling_params_cls = None
            del engine
            try:
                from vllm.distributed.parallel_state import (  # type: ignore
                    destroy_model_parallel,
                )

                destroy_model_parallel()
            except Exception:
                pass
            gc.collect()
            try:
                import torch

                if torch.cuda.is_available():
                    torch.cuda.synchronize()
                    torch.cuda.empty_cache()
                    torch.cuda.ipc_collect()
            except Exception:
                pass
