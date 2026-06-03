import torch
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer
)

class Agent:

    def __init__(self,model_path,system_prompt,device):
        self.device = device

        self.system_prompt = system_prompt

        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            trust_remote_code=True
        )

        self.model = AutoModelForCausalLM.from_pretrained(
            model_path,
            trust_remote_code=True,
            torch_dtype=torch.bfloat16
        ).to(device)

    @torch.no_grad()
    def chat(self,prompt,max_new_tokens=1024,temperature=0.7):
        messages = [
            {
                "role": "system",
                "content": self.system_prompt
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )

        inputs = self.tokenizer(
            text,
            return_tensors="pt"
        ).to(self.device)

        outputs = self.model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=True,
            temperature=temperature,
            pad_token_id=self.tokenizer.eos_token_id
        )

        answer = self.tokenizer.decode(
            outputs[0][inputs.input_ids.shape[1]:],
            skip_special_tokens=True
        )

        return answer

    def save(self, path):
        self.model.save_pretrained(path)
        self.tokenizer.save_pretrained(path)