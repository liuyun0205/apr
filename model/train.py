import torch


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