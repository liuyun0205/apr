import torch

import utils


class MultiTrainer:
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

    def build_matrices(self, candidates, exec_kwargs=None):

        naive_codes = candidates["naive_codes"]
        solver_codes = candidates["solver_codes"]
        inputs = candidates["inputs"]
        run_kw = exec_kwargs or {}
        all_matrices = []
        for input_case in inputs:
            matrix = []
            for solver_code in solver_codes:
                row = []
                solver_out, _ = utils.run_solve(
                    solver_code,
                    input_case,
                    **run_kw,
                )
                for naive_code in naive_codes:
                    naive_out, _ = utils.run_solve(
                        naive_code,
                        input_case,
                        **run_kw,
                    )
                    row.append(
                        1 if solver_out.strip() == naive_out.strip() else 0
                    )
                matrix.append(row)
            all_matrices.append(matrix)
        return all_matrices

    def calc_solver_rewards(self, all_matrices):
        """
        all_matrices[input_idx][solver_idx][naive_idx]
        """

        if not all_matrices:
            return []

        num_inputs = len(all_matrices)
        num_solvers = len(all_matrices[0])
        if num_solvers == 0:
            return []

        rewards = [0.0] * num_solvers

        for matrix in all_matrices:
            # 每个 solver 横向求和
            row_sums = [
                sum(row)
                for row in matrix
            ]

            max_sum = max(row_sums)

            if max_sum == 0:
                input_rewards = [0.0] * num_solvers
            else:
                input_rewards = [
                    row_sum / max_sum
                    for row_sum in row_sums
                ]

            for solver_idx in range(num_solvers):
                rewards[solver_idx] += input_rewards[solver_idx]

        # 多个 input 取平均
        rewards = [
            r / num_inputs
            for r in rewards
        ]

        return rewards

