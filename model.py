from __future__ import annotations

from config import get_args
from utils import file2text

import os

from LLM import LLM, LLMConfig, resolve_api_base_url


class model:
    def __init__(self, args):
        self.model_type = args.model_type
        self.model_name = args.basemodel
        self.prompt = file2text("prompt.txt")

        if self.model_type == "own":
            raise ValueError("model_type='own' 已被移除，请使用 'local' 或 'api'。")

        if self.model_type == "api":
            api_key = (getattr(args, "api_key", None) or "").strip() or os.getenv("OPENAI_API_KEY")
            self.llm = LLM(
                LLMConfig(
                    model_type="direct",
                    model=self.model_name,
                    system_prompt=self.prompt,
                    base_url=resolve_api_base_url(cli_base_url=(getattr(args, "base_url", None) or "")),
                    api_key=api_key,
                )
            )
            return

        self.llm = LLM(
            LLMConfig(
                model_type="local",
                model=self.model_name,
                system_prompt=self.prompt,
                tensor_parallel_size=getattr(args, "tensor_parallel_size", 1),
                gpu_memory_utilization=getattr(args, "gpu_memory_utilization", 0.9),
            )
        )

    def main(self, question: str) -> str:
        return self.llm.chat(question)


if __name__ == "__main__":
    args = get_args()
    m = model(args)
    print(m.main("你好，简单介绍一下你自己。"))