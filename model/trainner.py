from concurrent.futures import ProcessPoolExecutor, as_completed

import torch

import utils

class MultiTrainer:
    @staticmethod
    def _compare_cell(solver_out: str, solver_err: str, naive_out: str, naive_err: str) -> int:
        """失败/超时视为 0（不匹配 naive）。"""
        if not utils.run_solve_ok(solver_err) or not utils.run_solve_ok(naive_err):
            return 0
        return 1 if solver_out == naive_out else 0

    @staticmethod
    def _run_one(code: str, input_case: str, run_kw: dict, label: str) -> tuple[str, str]:
        timeout = int(run_kw.get("timeout", 10))
        stdout, stderr = utils.run_solve_plain(code, input_case, timeout=timeout)
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

    @staticmethod
    def _public_confidence(naive_public_pass: list) -> tuple[float, float]:
        """w = 通过 Public Test 的 Naive 比例；c = 2w - 1。"""
        n = len(naive_public_pass)
        if n == 0:
            return 0.0, 0.0
        w = sum(naive_public_pass) / n
        return w, 2.0 * w - 1.0

    def build_public_pass_flags(
        self,
        solver_codes,
        naive_codes,
        public_inputs,
        public_outputs,
        exec_kwargs=None,
    ) -> tuple[list[int], list[int]]:
        """
        在题干样例（Public Test）上评测，返回 (solver_pass, naive_pass)。
        各元素为 1/0：是否通过全部 Public Test。
        """
        run_kw = dict(exec_kwargs or {})
        run_kw.pop("exec_workers", None)
        has_public = bool(public_inputs) and len(public_inputs) == len(public_outputs or [])

        solver_pass = [
            1
            if has_public
            and utils.solver_passes_all_cases(code, public_inputs, public_outputs, **run_kw)
            else 0
            for code in solver_codes
        ]
        naive_pass = [
            1
            if has_public
            and utils.solver_passes_all_cases(code, public_inputs, public_outputs, **run_kw)
            else 0
            for code in naive_codes
        ]
        return solver_pass, naive_pass

    def _build_matrices_serial(
        self,
        solver_codes,
        naive_codes,
        inputs,
        run_kw,
    ):
        all_matrices = []
        for in_idx, input_case in enumerate(inputs):
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
            matrix = []
            for si, _solver_code in enumerate(solver_codes):
                row = []
                solver_out, solver_err = solver_out_map[(in_idx, si)]
                for ni, _naive_code in enumerate(naive_codes):
                    naive_out, naive_err = naive_out_map[(in_idx, ni)]
                    cell = self._compare_cell(
                        solver_out, solver_err, naive_out, naive_err
                    )
                    row.append(cell)
                matrix.append(row)
            all_matrices.append(matrix)
        return all_matrices

    def build_matrices(self, candidates, exec_kwargs=None):
        """在 m 条随机/rollout 输入上构建共识矩阵 M_j。"""
        naive_codes = candidates["naive_codes"]
        solver_codes = candidates["solver_codes"]
        inputs = candidates["inputs"]
        run_kw = dict(exec_kwargs or {})
        workers = int(run_kw.pop("exec_workers", 1) or 1)

        if workers <= 1:
            return self._build_matrices_serial(
                solver_codes, naive_codes, inputs, run_kw
            )

        return self._build_matrices_parallel(
            solver_codes,
            naive_codes,
            inputs,
            run_kw,
            workers,
        )



    def calc_solver_rewards(
        self,
        all_matrices,
        solver_public_pass,
        naive_public_pass,
        beta=0.3,
    ):
        """
        Solver-Naive Cooperative RL Reward（Solver 侧）。

        R_{s,i} = (1/m) Σ_j [ S_{i,j} + β·c·A_i ]

        - M_{i,k,j} = all_matrices[j][i][k]（随机输入 j 上输出是否一致）
        - S_{i,j} = Σ_k M_{i,k,j} / max_r Σ_k M_{r,k,j}
        - w = (#通过 Public Test 的 Naive) / N_n，c = 2w - 1
        - A_i = 1[solver_i 通过 Public Test]
        """
        if not all_matrices:
            return []

        num_inputs = len(all_matrices)
        num_solvers = len(all_matrices[0])
        if num_solvers == 0:
            return []

        _w, c_j = self._public_confidence(naive_public_pass)
        rewards = [0.0] * num_solvers

        for matrix in all_matrices:
            row_sums = [sum(row) for row in matrix]
            max_sum = max(row_sums) if row_sums else 0

            for solver_idx in range(num_solvers):
                if max_sum == 0:
                    s_ij = 0.0
                else:
                    s_ij = row_sums[solver_idx] / max_sum

                a_i = (
                    solver_public_pass[solver_idx]
                    if solver_idx < len(solver_public_pass)
                    else 0
                )
                rewards[solver_idx] += s_ij + beta * c_j * a_i

        return [r / num_inputs for r in rewards]

    def calc_naive_rewards(
        self,
        all_matrices,
        naive_public_pass,
        alpha=0.3,
    ):
        """
        Solver-Naive Cooperative RL Reward（Naive 侧）。

        R_{n,k} = (1/m) Σ_j [ C_{k,j} + α·c·N_k ]

        - C_{k,j} = Σ_i M_{i,k,j} / max_r Σ_i M_{i,r,j}
        - N_k = 1[naive_k 通过 Public Test]
        """
        if not all_matrices:
            return []

        num_inputs = len(all_matrices)
        num_solvers = len(all_matrices[0])
        num_naives = len(all_matrices[0][0])
        if num_naives == 0:
            return []

        _w, c_j = self._public_confidence(naive_public_pass)
        rewards = [0.0] * num_naives

        for matrix in all_matrices:
            col_sums = [
                sum(matrix[solver_idx][naive_idx] for solver_idx in range(num_solvers))
                for naive_idx in range(num_naives)
            ]
            max_col = max(col_sums) if col_sums else 0

            for naive_idx in range(num_naives):
                if max_col == 0:
                    c_kj = 0.0
                else:
                    c_kj = col_sums[naive_idx] / max_col

                n_k = (
                    naive_public_pass[naive_idx]
                    if naive_idx < len(naive_public_pass)
                    else 0
                )
                rewards[naive_idx] += c_kj + alpha * c_j * n_k

        return [r / num_inputs for r in rewards]
    