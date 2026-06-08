import logging
import os
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from agent import Agent
import utils
from LLM import LLM, LLMConfig

logger = logging.getLogger(__name__)


class VllmInputTrigger:
    """vLLM 模式下 input_trigger：用 trigger prompt 生成测例脚本（不占额外 HF 卡）。"""

    def __init__(self, model: "Model"):
        self._model = model

    def generate_code(self, question: str) -> str:
        codes = self._model._vllm_batch(
            self._model._prompt_trigger,
            question,
            1,
        )
        return codes[0] if codes else ""


class Model:

    def __init__(
        self,
        ds,
        model_path="/home/user/lzh/Qwen2.5-Coder-7B-Instruct",
        devices=None,
        lr=1e-5,
        use_lora: bool = False,
        lora_r: int = 64,
        lora_alpha: int = 128,
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
        self._use_lora = bool(use_lora)
        self._lora_r = int(lora_r)
        self._solver_lora_path: Optional[str] = None
        self._solver_lora_int_id: int = 1
        self._last_solver_gen_backend: str = ""
        self.rollout_lora_cache_dir: Optional[str] = None

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
                enable_lora=bool(use_lora),
                max_lora_rank=int(lora_r),
            )
        )

        if self.input_source == "input_output":
            self.input_trigger = None
        else:
            self.input_trigger = VllmInputTrigger(self)
            logger.info("input_trigger 已启用（vLLM + prompt/input_trriger.txt）")
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
            logger.info(
                "input_trigger 已启用（HF Agent @ %s）",
                trigger_dev,
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

    def _vllm_batch(
        self,
        system_prompt: str,
        question: str,
        n: int,
        *,
        lora_path: Optional[str] = None,
        lora_int_id: int = 1,
    ) -> list:
        if n <= 0:
            return []
        users = [question] * n
        return self._vllm.chat_batch(
            users,
            system_prompt=system_prompt,
            lora_path=lora_path,
            lora_int_id=lora_int_id,
        )

    def export_solver_lora(self, path: str) -> str:
        """将当前 solver LoRA 导出到目录，供 vLLM 验证加载。"""
        if not self._use_lora:
            raise RuntimeError("export_solver_lora 需要 --use_lora")
        out = Path(path).expanduser()
        out.mkdir(parents=True, exist_ok=True)
        self.solver.save(str(out))
        if not (out / "adapter_config.json").exists():
            raise RuntimeError(
                f"LoRA 导出失败，缺少 adapter_config.json: {out}"
            )
        return str(out)

    def sync_solver_lora_for_vllm(self, cache_dir: str, *, lora_int_id: int) -> str:
        """导出当前 HF LoRA 到 cache_dir，供 vLLM rollout/验证加载（与内存权重一致）。"""
        path = self.export_solver_lora(cache_dir)
        self.set_solver_lora_snapshot(path, lora_int_id=lora_int_id)
        return path

    def clear_solver_lora_snapshot(self) -> None:
        self.set_solver_lora_snapshot(None)

    def set_solver_lora_snapshot(
        self,
        path: Optional[str],
        *,
        lora_int_id: int = 1,
    ) -> None:
        """设置 vLLM LoRA 快照路径；None 表示清除。"""
        self._solver_lora_path = str(path).strip() if path else None
        self._solver_lora_int_id = max(1, int(lora_int_id))

    def resolve_solver_gen_backend(self, *, use_trainable_solver: bool) -> str:
        """
        返回 solver 代码生成后端标签：
          hf_lora / vllm_lora — 可训练 LoRA（正确路径）
          hf_full — 全参 HF（无 LoRA 但仍在更新 HF 权重）
          vllm_base / hf_base — 冻结基座（训练 solver 时禁止使用）
        """
        if not use_trainable_solver:
            if self.chat_backend == "vllm":
                return "vllm_base"
            return "hf_base"

        if self.chat_backend == "vllm" and self._solver_lora_path:
            return "vllm_lora"
        if self._use_lora or getattr(self.solver, "use_lora", False):
            return "hf_lora"
        return "hf_full"

    def ensure_solver_not_base(self, *, context: str) -> str:
        """训练/验证 rollout 禁止走冻结基座，否则等于原地踏步。"""
        backend = self._last_solver_gen_backend or "unknown"
        if backend in ("vllm_base", "hf_base"):
            raise RuntimeError(
                f"{context} 的 solver 生成误用冻结基座 ({backend})。"
                "请确认 --use_lora，且 rollout/验证未关闭 use_trainable_solver。"
            )
        if backend == "hf_full" and self.chat_backend == "vllm":
            logger.warning(
                "%s: solver 走 HF 全参微调（未启用 LoRA），显存占用大",
                context,
            )
        return backend

    def _generate_solver_codes_impl(
        self,
        question: str,
        n: int,
        *,
        use_trainable_solver: bool,
    ) -> list:
        n = max(1, int(n))
        backend = self.resolve_solver_gen_backend(
            use_trainable_solver=use_trainable_solver
        )
        self._last_solver_gen_backend = backend

        if backend == "vllm_base":
            return self._vllm_batch(self._prompt_solver, question, n)
        if backend == "vllm_lora":
            assert self._solver_lora_path
            return self._vllm_batch(
                self._prompt_solver,
                question,
                n,
                lora_path=self._solver_lora_path,
                lora_int_id=self._solver_lora_int_id,
            )
        return [self.solver.chat(question) for _ in range(n)]

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
        *,
        use_trainable_solver: bool = True,
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
            if use_trainable_solver:
                solver_codes = self._generate_solver_codes_impl(
                    question,
                    solver_bestofn,
                    use_trainable_solver=True,
                )
            else:
                solver_codes = self._generate_solver_codes_impl(
                    question,
                    solver_bestofn,
                    use_trainable_solver=False,
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

    def generate_solver_codes(
        self,
        question: str,
        n: int = 1,
        *,
        use_trainable_solver: bool = True,
    ) -> list:
        """仅生成 solver 代码（用于 pass@1 等独立评测，与 bestofN 批次无关）。"""
        return self._generate_solver_codes_impl(
            question,
            n,
            use_trainable_solver=use_trainable_solver,
        )

    def generate_input(self, question, count=10):
        if self.input_trigger is None:
            return []
        if isinstance(self.input_trigger, VllmInputTrigger):
            code = self.input_trigger.generate_code(question)
        else:
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
