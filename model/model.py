import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from agent import Agent
import utils
from LLM import LLM, LLMConfig

logger = logging.getLogger(__name__)


class Model:

    def __init__(
        self,
        ds,
        model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
        devices=None,
        lr=1e-5,
        use_lora: bool = False,
        lora_r: int = 8,
        lora_alpha: int = 16,
        lora_dropout: float = 0.05,
        gradient_checkpointing: bool = False,
        exec_kwargs=None,
        chat_backend: str = "hf",
        vllm_tp_size: int = 2,
        vllm_gpu_memory_utilization: float = 0.9,
        max_new_tokens: int = 1024,
        temperature: float = 0.7,
        input_source: str = "trigger",
    ):
        self._exec_kwargs = exec_kwargs or {}
        self.dataset = ds
        self.chat_backend = (chat_backend or "hf").strip().lower()
        self.input_source = (input_source or "trigger").strip().lower()

        self._prompt_trigger = utils.file2text("prompt/input_trriger.txt")
        self._prompt_naive = utils.file2text("prompt/naivesolver.txt")
        self._prompt_solver = utils.file2text("prompt/solver.txt")

        if self.chat_backend == "vllm":
            self._init_vllm_backend(
                model_path=model_path,
                vllm_tp_size=vllm_tp_size,
                vllm_gpu_memory_utilization=vllm_gpu_memory_utilization,
                max_new_tokens=max_new_tokens,
                temperature=temperature,
                devices=devices,
                model_path_train=model_path,
                lr=lr,
                use_lora=use_lora,
                lora_r=lora_r,
                lora_alpha=lora_alpha,
                lora_dropout=lora_dropout,
                gradient_checkpointing=gradient_checkpointing,
            )
        elif self.chat_backend == "hf":
            self._init_hf_backend(
                model_path=model_path,
                devices=devices,
                lr=lr,
                use_lora=use_lora,
                lora_r=lora_r,
                lora_alpha=lora_alpha,
                lora_dropout=lora_dropout,
                gradient_checkpointing=gradient_checkpointing,
            )
        else:
            raise ValueError(f"未知 chat_backend: {chat_backend!r}，可选 hf / vllm")

    def _init_vllm_backend(
        self,
        *,
        model_path,
        vllm_tp_size,
        vllm_gpu_memory_utilization,
        max_new_tokens,
        temperature,
        devices,
        model_path_train,
        lr,
        use_lora,
        lora_r,
        lora_alpha,
        lora_dropout,
        gradient_checkpointing,
    ):
        """单实例 vLLM 批量推理；solver 训练仍用 HF Agent（单卡）。"""
        if devices is None:
            solver_dev = "cuda:0"
        elif isinstance(devices, str):
            parts = [d.strip() for d in devices.split(",") if d.strip()]
            solver_dev = parts[-1] if parts else "cuda:0"
        else:
            solver_dev = devices[-1] if devices else "cuda:0"

        logger.info(
            "chat_backend=vllm: tp_size=%d solver_train_device=%s",
            vllm_tp_size,
            solver_dev,
        )

        self._vllm = LLM(
            LLMConfig(
                model_type="local",
                model=model_path,
                model_path=model_path,
                system_prompt="",
                tensor_parallel_size=max(1, int(vllm_tp_size)),
                gpu_memory_utilization=float(vllm_gpu_memory_utilization),
                max_new_tokens=max_new_tokens,
                temperature=temperature,
            )
        )

        self.input_trigger = None
        self.naivesolver = None
        self.solver = Agent(
            model_path=model_path_train,
            system_prompt=self._prompt_solver,
            device=solver_dev,
            lr=lr,
            trainable=True,
            use_lora=use_lora,
            lora_r=lora_r,
            lora_alpha=lora_alpha,
            lora_dropout=lora_dropout,
            gradient_checkpointing=gradient_checkpointing,
        )

    def _init_hf_backend(
        self,
        *,
        model_path,
        devices,
        lr,
        use_lora,
        lora_r,
        lora_alpha,
        lora_dropout,
        gradient_checkpointing,
    ):
        if devices is None:
            devices = ("cuda:0", "cuda:1", "cuda:2")
        elif isinstance(devices, str):
            devices = tuple(d.strip() for d in devices.split(","))
        if len(devices) != 3:
            raise ValueError("hf 模式需要 3 个 device")
        trigger_dev, naive_dev, solver_dev = devices

        lora_kwargs = dict(
            lora_r=lora_r,
            lora_alpha=lora_alpha,
            lora_dropout=lora_dropout,
            gradient_checkpointing=gradient_checkpointing,
        )

        self._vllm = None
        if self.input_source == "input_output":
            self.input_trigger = None
        else:
            self.input_trigger = Agent(
                model_path=model_path,
                system_prompt=self._prompt_trigger,
                device=trigger_dev,
                trainable=False,
                use_lora=False,
            )
        self.naivesolver = Agent(
            model_path=model_path,
            system_prompt=self._prompt_naive,
            device=naive_dev,
            trainable=False,
            use_lora=False,
        )
        self.solver = Agent(
            model_path=model_path,
            system_prompt=self._prompt_solver,
            device=solver_dev,
            lr=lr,
            trainable=True,
            use_lora=use_lora,
            **lora_kwargs,
        )

    def _vllm_batch(self, system_prompt: str, question: str, n: int) -> list:
        if n <= 0:
            return []
        users = [question] * n
        return self._vllm.chat_batch(users, system_prompt=system_prompt)

    def _load_inputs_from_dataset(self, idx: int, count: int) -> list:
        getter = getattr(self.dataset, "get_io_inputs", None)
        if not callable(getter):
            return []
        try:
            return list(getter(idx, max_count=count))
        except (FileNotFoundError, OSError):
            return []

    def resolve_inputs(self, question: str, count: int, idx=None) -> tuple:
        """
        返回 (inputs, source_tag)。
        source_tag: input_output | trigger
        """
        if self.input_source in ("input_output", "auto") and idx is not None:
            ins = self._load_inputs_from_dataset(idx, count)
            if ins:
                return ins, "input_output"

        if self.input_source == "input_output":
            return [], "input_output"

        return self.generate_input(question, count=count), "trigger"

    def generate_candidates(
        self,
        naive_bestofn,
        solver_bestofn,
        question,
        input_count=10,
        idx=None,
    ):
        outs, input_src = self.resolve_inputs(question, input_count, idx=idx)
        logger.info(
            "测例来源: %s，共 %d 条（idx=%s）",
            input_src,
            len(outs),
            idx,
        )

        if self.chat_backend == "vllm":
            naive_codes = self._vllm_batch(
                self._prompt_naive, question, naive_bestofn
            )
            solver_codes = self._vllm_batch(
                self._prompt_solver, question, solver_bestofn
            )
        else:
            naive_codes = []
            solver_codes = []
            for _ in range(naive_bestofn):
                naive_codes.append(self.naivesolver.chat(question))
            for _ in range(solver_bestofn):
                solver_codes.append(self.solver.chat(question))

        return {
            "naive_codes": naive_codes,
            "solver_codes": solver_codes,
            "inputs": outs,
            "input_source": input_src,
        }

    def generate_solver_codes(self, question: str, n: int = 1) -> list:
        """仅生成 solver 代码（用于 pass@1 等独立评测，与 bestofN 批次无关）。"""
        n = max(1, int(n))
        if self.chat_backend == "vllm":
            return self._vllm_batch(self._prompt_solver, question, n)
        return [self.solver.chat(question) for _ in range(n)]

    def generate_input(self, question, count=10):
        if self.chat_backend == "vllm":
            code = self._vllm_batch(self._prompt_trigger, question, 1)[0]
        else:
            if self.input_trigger is None:
                return []
            code = self.input_trigger.chat(question)

        code = utils.clean_code(code)
        stdout, _stderr = utils.run_code(code, **self._exec_kwargs)
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        return lines[:count]


if __name__ == "__main__":
    from alldatasets.loader import load_dataset

    dataset = load_dataset("apps", "~/get_codeforces_data/APPS/train")
    print("数据集加载完成!")
    model = Model(dataset, chat_backend="vllm", devices="cuda:2", vllm_tp_size=2)
