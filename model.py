from __future__ import annotations

from config import get_args
from utils import file2text

from LLM import LLM, LLMConfig


class model:
    def __init__(self, args):
        self.model_type = args.model_type
        self.model_name = args.basemodel
        self.prompt = file2text("prompt.txt")

        if self.model_type == "own":
            raise ValueError("model_type='own' 已被移除，请使用 'local' 或 'api'。")

        self.llm = LLM(
            LLMConfig(
                model_type=self.model_type,
                model=self.model_name,
                system_prompt=self.prompt,
                use_zero=getattr(args, "use_zero", False),
                zero_stage=getattr(args, "zero_stage", 3),
                zero_offload=getattr(args, "zero_offload", "none"),
            )
        )

    def main(self, question: str) -> str:
        return self.llm.chat(question)


if __name__ == "__main__":
    args = get_args()
    m = model(args)
    print(m.main("你好，简单介绍一下你自己。"))