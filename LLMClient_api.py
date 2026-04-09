import time

import openai
from langchain.chat_models import init_chat_model
import os
from dotenv import load_dotenv
from langchain_core.globals import set_llm_cache
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, BaseMessage
import utils
from api_manager import ExcelKeyTable
from ip_controller import switch_ip

openai.default_headers = {"x-foo": "true"}
class LLMClient_api:
    def __init__(self, content, last_turns=3, model="gpt-5-mini"):
        set_llm_cache(None)
        load_dotenv()
        self.apiManager = ExcelKeyTable()
        self.apiManager.ensure_today_env_key()
        self.model=self.build_model(model)
        self.content=content
        self.history=[
            SystemMessage(content=self.content)
        ]
        self.modelname=model
        self.last_turns=last_turns
        self.draft=[]

        print(f"key:{os.environ.get(self.apiManager.envkey)}")

    def query(self, text, temperature=0.5, timeout=180, memory=True):
        print(f"\nQuestion:--------------------------------\n{text}")

        # 1. 构建对话窗口
        if memory:
            self.history.append(HumanMessage(content=text))
            window = self.build_window_last_turns(self.history, last_turns=self.last_turns)
        else:
            system_msgs = [m for m in self.history if isinstance(m, SystemMessage)]
            window = system_msgs + [HumanMessage(content=text)]

        last_error = None

        for attempt in range(4):
            key = os.environ.get(self.apiManager.envkey)
            self.apiManager.keyuse(key)

            # 绑定参数
            m = self.model.bind(temperature=temperature, timeout=timeout)

            try:
                content = ""
                print("Answer: ", end="", flush=True)

                for chunk in m.stream(window):
                    part = chunk.content if hasattr(chunk, "content") else str(chunk)
                    content += part
                    print(part, end="", flush=True)
                print("")


                ok, used, remain = self.apiManager.keycheck()
                if not ok:
                    switch_ip()
                    time.sleep(1)
                    self.apiManager.next_key()
                    self.rebuild()
                    continue

                if memory:
                    self.history.append(AIMessage(content=content))
                return content

            except Exception as e:
                last_error = e
                print(f"\n[Error] {e}")  # 打印错误换行
                ok, used, remain = self.apiManager.keycheck()
                if not ok:
                    switch_ip()
                    time.sleep(1)
                    self.apiManager.next_key()
                else:
                    switch_ip()
                    time.sleep(0.5)

                self.rebuild()

        error_msg = f"Error: {last_error}"
        return error_msg

    def setRole(self, prompt_path):
        content = utils.read_text_safely(prompt_path)
        self.content = content
        # 如果第 0 条就是 System，替换；否则插入到最前面
        if self.history and isinstance(self.history[0], SystemMessage):
            self.history[0] = SystemMessage(content=self.content)
        else:
            self.history.insert(0, SystemMessage(content=self.content))
    def drop_all_ai(self):
        self.history = [m for m in self.history if not isinstance(m, AIMessage)]
    def drop_all(self):
        self.history = [m for m in self.history if isinstance(m, SystemMessage)]
    def build_window_last_turns(self,history, last_turns):
        systems = [m for m in history if isinstance(m, SystemMessage)]
        others = [m for m in history if not isinstance(m, SystemMessage)]

        keep_n = 2 * max(1, last_turns)
        window_others = others[-keep_n:] if len(others) > keep_n else others
        return systems + window_others

    def build_model(self,model):
        return init_chat_model(
            model=model,
            api_key=os.environ.get(self.apiManager.envkey),
            timeout=180,
            max_retries=0,
            model_provider="openai"
        )
    def rebuild(self):
        if hasattr(self.model, "close"):
            try: self.model.close()
            except Exception: pass
        import time;
        time.sleep(1.5)
        self.model = self.build_model(self.modelname)
        print(f"[rebuild] key now: {os.environ.get(self.apiManager.envkey)}")
if __name__ == "__main__":
    llm=LLMClient_api("hello")
    llm.query("hello")
