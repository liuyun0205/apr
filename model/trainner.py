import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import torch

import utils

logger = logging.getLogger(__name__)

class MultiTrainer:
    _LOG_PREVIEW = 500

    @staticmethod
    def _preview_text(text: str, limit: int | None = None) -> str:
        limit = limit or MultiTrainer._LOG_PREVIEW
        t = (text or "").replace("\r\n", "\n")
        if len(t) <= limit:
            return t
        return t[:limit] + f"... ({len(t)} chars)"

    @staticmethod
    def _log_exec_result(
        role: str,
        input_idx: int,
        cand_idx: int,
        stdout: str,
        stderr: str,
        *,
        match: int | None = None,
    ) -> None:
        status = "ok" if utils.run_solve_ok(stderr) else (stderr or "fail")
        extra = f" match={match}" if match is not None else ""
        logger.info(
            "[%s] input=%d cand=%d status=%s%s\nstdout:\n%s\nstderr: %s",
            role,
            input_idx,
            cand_idx,
            status,
            extra,
            MultiTrainer._preview_text(stdout),
            (stderr or "(empty)")[:300],
        )

    @staticmethod
    def _compare_cell(solver_out: str, solver_err: str, naive_out: str, naive_err: str) -> int:
        """失败/超时视为 0（不匹配 naive）。"""
        if not utils.run_solve_ok(solver_err) or not utils.run_solve_ok(naive_err):
            return 0
        return 1 if solver_out == naive_out else 0

    @staticmethod
    def _run_one(code: str, input_case: str, run_kw: dict, label: str) -> tuple[str, str]:
        stdout, stderr = utils.run_solve(
            code,
            input_case,
            exec_label=label,
            **run_kw,
        )
        return stdout.strip(), stderr or ""

    def get_logprob(self, agent, prompt, code):
        device = agent.device
        tokenizer = agent.tokenizer
        model = agent.model

        text = prompt + code

        enc = tokenizer(
            text,
            return_tensors="pt"
        ).to(device)

        prompt_enc = tokenizer(
            prompt,
            return_tensors="pt"
        ).to(device)

        input_ids = enc["input_ids"]
        prompt_len = prompt_enc["input_ids"].shape[1]

        outputs = model(input_ids=input_ids)

        logits = outputs.logits[:, :-1, :]
        labels = input_ids[:, 1:]

        log_probs = torch.log_softmax(logits, dim=-1)

        token_log_probs = log_probs.gather(
            dim=-1,
            index=labels.unsqueeze(-1)
        ).squeeze(-1)

        completion_log_probs = token_log_probs[:, prompt_len - 1:]

        return completion_log_probs.mean()

    def update_agent(self, agent, prompt, code, reward):
        if agent.optimizer is None:
            raise RuntimeError("该 Agent 未启用训练（optimizer=None）")

        agent.model.train()

        advantage = torch.tensor(
            reward,
            dtype=torch.float32,
            device=agent.device
        )

        logprob = self.get_logprob(
            agent=agent,
            prompt=prompt,
            code=code
        )

        loss = -advantage * logprob

        agent.optimizer.zero_grad()
        loss.backward()

        torch.nn.utils.clip_grad_norm_(
            agent.model.parameters(),
            max_norm=1.0
        )

        agent.optimizer.step()

        return loss.item()

    def _build_matrices_serial(
        self,
        solver_codes,
        naive_codes,
        inputs,
        run_kw,
    ):
        all_matrices = []
        for in_idx, input_case in enumerate(inputs):
            logger.info(
                "--- 测例 input=%d len=%d ---\n%s",
                in_idx,
                len(input_case),
                self._preview_text(repr(input_case), limit=300),
            )
            matrix = []
            for si, solver_code in enumerate(solver_codes):
                row = []
                solver_out, solver_err = self._run_one(
                    solver_code, input_case, run_kw, "solver"
                )
                for ni, naive_code in enumerate(naive_codes):
                    naive_out, naive_err = self._run_one(
                        naive_code, input_case, run_kw, "naive"
                    )
                    cell = self._compare_cell(
                        solver_out, solver_err, naive_out, naive_err
                    )
                    if ni == 0:
                        self._log_exec_result(
                            "solver", in_idx, si, solver_out, solver_err
                        )
                    if si == 0:
                        self._log_exec_result(
                            "naive", in_idx, ni, naive_out, naive_err
                        )
                    logger.info(
                        "[match] input=%d solver=%d naive=%d -> %d",
                        in_idx,
                        si,
                        ni,
                        cell,
                    )
                    row.append(cell)
                matrix.append(row)
            all_matrices.append(matrix)
        return all_matrices

    def _build_matrices_parallel(
        self,
        solver_codes,
        naive_codes,
        inputs,
        run_kw,
        workers: int,
    ):
        solver_tasks = []
        naive_tasks = []
        for in_idx, input_case in enumerate(inputs):
            for si, solver_code in enumerate(solver_codes):
                solver_tasks.append(
                    (in_idx, si, solver_code, input_case, "solver")
                )
            for ni, naive_code in enumerate(naive_codes):
                naive_tasks.append(
                    (in_idx, ni, naive_code, input_case, "naive")
                )

        solver_out_map = {}
        naive_out_map = {}

        pool_kw = dict(max_workers=workers)
        with ProcessPoolExecutor(**pool_kw) as pool:
            fut_solver = {
                pool.submit(
                    utils._run_solve_worker,
                    (code, inp, {**run_kw, "exec_label": role}),
                ): (in_idx, si)
                for in_idx, si, code, inp, role in solver_tasks
            }
            for fut in as_completed(fut_solver):
                in_idx, si = fut_solver[fut]
                solver_out_map[(in_idx, si)] = fut.result()

            fut_naive = {
                pool.submit(
                    utils._run_solve_worker,
                    (code, inp, {**run_kw, "exec_label": role}),
                ): (in_idx, ni)
                for in_idx, ni, code, inp, role in naive_tasks
            }
            for fut in as_completed(fut_naive):
                in_idx, ni = fut_naive[fut]
                naive_out_map[(in_idx, ni)] = fut.result()

        all_matrices = []
        for in_idx, input_case in enumerate(inputs):
            logger.info(
                "--- 测例 input=%d len=%d ---\n%s",
                in_idx,
                len(input_case),
                self._preview_text(repr(input_case), limit=300),
            )
            for ni in range(len(naive_codes)):
                naive_out, naive_err = naive_out_map[(in_idx, ni)]
                self._log_exec_result(
                    "naive", in_idx, ni, naive_out, naive_err
                )
            matrix = []
            for si, _solver_code in enumerate(solver_codes):
                row = []
                solver_out, solver_err = solver_out_map[(in_idx, si)]
                self._log_exec_result(
                    "solver", in_idx, si, solver_out, solver_err
                )
                for ni, _naive_code in enumerate(naive_codes):
                    naive_out, naive_err = naive_out_map[(in_idx, ni)]
                    cell = self._compare_cell(
                        solver_out, solver_err, naive_out, naive_err
                    )
                    logger.info(
                        "[match] input=%d solver=%d naive=%d -> %d",
                        in_idx,
                        si,
                        ni,
                        cell,
                    )
                    row.append(cell)
                matrix.append(row)
            all_matrices.append(matrix)
        return all_matrices

    def build_matrices(self, candidates, exec_kwargs=None):
        naive_codes = candidates["naive_codes"]
        solver_codes = candidates["solver_codes"]
        inputs = candidates["inputs"]
        run_kw = dict(exec_kwargs or {})
        workers = int(run_kw.pop("exec_workers", 1) or 1)

        if workers <= 1:
            return self._build_matrices_serial(
                solver_codes, naive_codes, inputs, run_kw
            )

        logger.info(
            "build_matrices 多进程执行: workers=%d tasks=%d",
            workers,
            len(inputs) * (len(solver_codes) + len(naive_codes)),
        )
        return self._build_matrices_parallel(
            solver_codes,
            naive_codes,
            inputs,
            run_kw,
            workers,
        )

    def calc_solver_rewards(self, all_matrices):
        """
        all_matrices[input_idx][solver_idx][naive_idx]，格为 1/0。
        执行失败或超时在矩阵里记 0。
        """

        if not all_matrices:
            return []

        num_inputs = len(all_matrices)
        num_solvers = len(all_matrices[0])
        if num_solvers == 0:
            return []

        rewards = [0.0] * num_solvers

        for matrix in all_matrices:
            row_sums = [sum(row) for row in matrix]
            max_sum = max(row_sums)

            if max_sum == 0:
                input_rewards = [0.0] * num_solvers
            else:
                input_rewards = [row_sum / max_sum for row_sum in row_sums]

            for solver_idx in range(num_solvers):
                rewards[solver_idx] += input_rewards[solver_idx]

        return [r / num_inputs for r in rewards]

