import argparse
from openai import OpenAI
from transformers import AutoModelForCausalLM, AutoTokenizer
from LLMClient_api import LLMClient_api
from config import get_args
from utils import file2text
args=get_args()
class model():
    def __init__(self,args):
        self.model_type = args.model_type
        self.model_name = args.basemodel
        self.prompt=file2text("prompt.txt")
        if args.model_type == 'api':
            self.client = OpenAI()
            self.model = args.basemodel
        elif args.model_type == 'local':
            self.tokenizer = AutoTokenizer.from_pretrained(args.basemodel)
            self.model = AutoModelForCausalLM.from_pretrained(
                args.basemodel,
                device_map="auto"
            )
        elif args.model_type=='own':
            self.client=LLMClient_api(self.prompt)
    def main(self,question):

        if self.model_type == "api":
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content":self.prompt},
                    {"role": "user", "content": question}
                ]
            )
            return response.choices[0].message.content

        elif args.model_type=='local':
            inputs = self.tokenizer(
                question,
                return_tensors="pt"
            ).to(self.model.device)

            outputs = self.model.generate(
                **inputs,
                max_new_tokens=512,
                do_sample=True,
                temperature=0.7
            )

            result = self.tokenizer.decode(
                outputs[0],
                skip_special_tokens=True
            )

            return result
        elif args.model_type=='own':
            res=self.client.query(question,memory=False)
            return res