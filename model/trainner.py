import math
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

    def _completion_token_logprobs(self, agent, prompt, code):
        device = agent.device
        tokenizer = agent.tokenizer
        model = agent.model

        text = prompt + code
        enc = tokenizer(text, return_tensors="pt").to(device)
        prompt_enc = tokenizer(prompt, return_tensors="pt").to(device)

        input_ids = enc["input_ids"]
        prompt_len = prompt_enc["input_ids"].shape[1]

        outputs = model(input_ids=input_ids)
        logits = outputs.logits[:, :-1, :]
        labels = input_ids[:, 1:]
        log_probs = torch.log_softmax(logits, dim=-1)
        token_log_probs = log_probs.gather(
            dim=-1,
            index=labels.unsqueeze(-1),
        ).squeeze(-1)
        return token_log_probs[:, prompt_len - 1:]

    def get_logprob_sum(self, agent, prompt, code):
        token_log_probs = self._completion_token_logprobs(agent, prompt, code)
        return token_log_probs.sum()

    def get_logprob(self, agent, prompt, code):
        token_log_probs = self._completion_token_logprobs(agent, prompt, code)
        return token_log_probs.mean()

    @staticmethod
    def normalize_advantages(rewards: list[float]) -> list[float]:
        if len(rewards) <= 1:
            return list(rewards)
        mean = sum(rewards) / len(rewards)
        var = sum((r - mean) ** 2 for r in rewards) / len(rewards)
        std = math.sqrt(var) if var > 1e-8 else 1.0
        return [(r - mean) / std for r in rewards]

    def update_agent_ppo(
        self,
        agent,
        prompt,
        code,
        advantage,
        old_logprob_sum,
        *,
        clip_eps: float = 0.2,
        ppo_epochs: int = 4,
    ):
        """PPO clipped objective：在固定 old_logprob 上多轮更新。"""
        if agent.optimizer is None:
            raise RuntimeError("该 Agent 未启用训练（optimizer=None）")

        agent.model.train()
        adv = torch.tensor(advantage, dtype=torch.float32, device=agent.device)
        old_lp = old_logprob_sum.detach()

        last_loss = 0.0
        for _ in range(max(1, int(ppo_epochs))):
            agent.optimizer.zero_grad()
            new_lp = self.get_logprob_sum(agent, prompt, code)
            ratio = torch.exp(new_lp - old_lp)
            surr1 = ratio * adv
            surr2 = torch.clamp(ratio, 1.0 - clip_eps, 1.0 + clip_eps) * adv
            loss = -torch.min(surr1, surr2)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(
                agent.model.parameters(),
                max_norm=1.0,
            )
            agent.optimizer.step()
            last_loss = loss.item()
        return last_loss

    def ppo_update_role(
        self,
        agent,
        prompt,
        codes,
        rewards,
        *,
        min_reward: float = 0.0,
        clip_eps: float = 0.2,
        ppo_epochs: int = 4,
        normalize_adv: bool = True,
    ) -> tuple[list[float], int]:
        """对同一角色的一批样本做 PPO 更新，返回 (losses, updated_count)。"""
        pairs: list[tuple[str, float]] = []
        for code, reward in zip(codes, rewards):
            if reward <= min_reward:
                continue
            code = utils.clean_code(code)
            if not code.strip():
                continue
            pairs.append((code, reward))
        if not pairs:
            return [], 0

        raw_rewards = [r for _, r in pairs]
        advantages = (
            self.normalize_advantages(raw_rewards)
            if normalize_adv
            else raw_rewards
        )

        was_training = agent.model.training
        agent.model.eval()
        old_logprobs = []
        with torch.no_grad():
            for code, _ in pairs:
                old_logprobs.append(self.get_logprob_sum(agent, prompt, code))
        if was_training:
            agent.model.train()

        losses: list[float] = []
        for (code, _), adv, old_lp in zip(pairs, advantages, old_logprobs):
            loss = self.update_agent_ppo(
                agent,
                prompt,
                code,
                adv,
                old_lp,
                clip_eps=clip_eps,
                ppo_epochs=ppo_epochs,
            )
            losses.append(loss)
        return losses, len(losses)

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



    @staticmethod
    def _gt_match_cell(out: str, err: str, expected: str) -> int:
        if not utils.run_solve_ok(err):
            return 0
        return 1 if utils.outputs_match(out, expected) else 0

    def _build_gt_match_matrix_serial(
        self,
        codes,
        inputs,
        expected_outputs,
        run_kw,
    ):
        matrix = []
        for code in codes:
            row = []
            for inp, exp in zip(inputs, expected_outputs):
                out, err = self._run_one(code, inp, run_kw, "gt")
                row.append(self._gt_match_cell(out, err, exp))
            matrix.append(row)
        return matrix

    def _build_gt_match_matrix_parallel(
        self,
        codes,
        inputs,
        expected_outputs,
        run_kw,
        workers: int,
    ):
        tasks = []
        for ci, code in enumerate(codes):
            for in_idx, (inp, exp) in enumerate(zip(inputs, expected_outputs)):
                tasks.append((ci, in_idx, code, inp, exp))

        cell_map = {}
        pool_kw = dict(max_workers=workers)
        with ProcessPoolExecutor(**pool_kw) as pool:
            fut_map = {
                pool.submit(
                    utils._run_solve_worker,
                    (code, inp, {**run_kw, "exec_label": "gt"}),
                ): (ci, in_idx, exp)
                for ci, in_idx, code, inp, exp in tasks
            }
            for fut in as_completed(fut_map):
                ci, in_idx, exp = fut_map[fut]
                out, err = fut.result()
                cell_map[(ci, in_idx)] = self._gt_match_cell(out, err, exp)

        return [
            [cell_map[(ci, in_idx)] for in_idx in range(len(inputs))]
            for ci in range(len(codes))
        ]

    def build_gt_match_matrix(
        self,
        codes,
        gt_inputs,
        gt_outputs,
        exec_kwargs=None,
    ):
        """返回 [num_codes][num_gt]：各代码在 GT 测例上的逐条匹配 0/1。"""
        run_kw = dict(exec_kwargs or {})
        workers = int(run_kw.pop("exec_workers", 1) or 1)
        if not codes or not gt_inputs or len(gt_inputs) != len(gt_outputs):
            return []

        if workers <= 1:
            return self._build_gt_match_matrix_serial(
                codes, gt_inputs, gt_outputs, run_kw
            )
        return self._build_gt_match_matrix_parallel(
            codes,
            gt_inputs,
            gt_outputs,
            run_kw,
            workers,
        )

    @staticmethod
    def estimate_delta_agreement(all_matrices, solver_gt_match, eps=1e-6):
        """
        Δ_A = log((P(G=1|A=1)+ε) / (P(G=1|A=0)+ε))
        在 solver×naive×input 粒度上统计。
        """
        g1_a1 = g1_a0 = 0.0
        n_a1 = n_a0 = 0
        for in_idx, matrix in enumerate(all_matrices):
            for solver_idx, row in enumerate(matrix):
                g = (
                    solver_gt_match[solver_idx][in_idx]
                    if solver_idx < len(solver_gt_match)
                    and in_idx < len(solver_gt_match[solver_idx])
                    else 0
                )
                for cell in row:
                    if cell:
                        n_a1 += 1
                        g1_a1 += g
                    else:
                        n_a0 += 1
                        g1_a0 += g
        if n_a1 == 0 and n_a0 == 0:
            return 0.0
        p_g_a1 = g1_a1 / n_a1 if n_a1 else 0.5
        p_g_a0 = g1_a0 / n_a0 if n_a0 else 0.5
        return math.log((p_g_a1 + eps) / (p_g_a0 + eps))

    def calc_solver_rewards(
        self,
        all_matrices,
        solver_gt_match,
        *,
        alpha=1.0,
        beta=0.3,
        eps=1e-6,
    ):
        """
        Information-Guided Agreement Reward（Solver 侧，无 D/C 正则项）。

        R_S = α·G(c) + β·Δ_A·A(c)

        - G(c) = (1/m) Σ_j 1[S(t_j)=y_j]（CURE hidden GT）
        - A(c) = (1/(N_n·m)) Σ_k Σ_j 1[S(t_j)=N_k(t_j)]
        - Δ_A 由 batch 估计 P(G=1|A=1) 与 P(G=1|A=0)
        """
        if not all_matrices or not solver_gt_match:
            return []

        num_solvers = len(all_matrices[0])
        if num_solvers == 0:
            return []

        delta_a = self.estimate_delta_agreement(
            all_matrices, solver_gt_match, eps=eps
        )
        rewards = []
        for solver_idx in range(num_solvers):
            gt_row = solver_gt_match[solver_idx]
            g_c = sum(gt_row) / len(gt_row) if gt_row else 0.0

            agree_sum = 0.0
            agree_cnt = 0
            for matrix in all_matrices:
                row = matrix[solver_idx]
                agree_sum += sum(row)
                agree_cnt += len(row)
            a_c = agree_sum / agree_cnt if agree_cnt else 0.0

            rewards.append(alpha * g_c + beta * delta_a * a_c)
        return rewards

    def calc_solver_gt_rewards(
        self,
        solver_gt_match,
        *,
        alpha: float = 1.0,
    ):
        """验证用：仅 GT 通过率奖励 R = α·G(c)（无 naive Agreement 项）。"""
        if not solver_gt_match:
            return []
        return [
            alpha * (sum(row) / len(row) if row else 0.0)
            for row in solver_gt_match
        ]

    def calc_naive_rewards(
        self,
        naive_codes,
        gt_inputs,
        gt_outputs,
        exec_kwargs=None,
    ):
        """
        Naive 侧：直接用 CURE hidden GT 通过率。

        R_N = (1/m) Σ_j 1[N(t_j)=y_j]
        """
        matrix = self.build_gt_match_matrix(
            naive_codes, gt_inputs, gt_outputs, exec_kwargs=exec_kwargs
        )
        if not matrix:
            return []
        return [sum(row) / len(row) if row else 0.0 for row in matrix]
    