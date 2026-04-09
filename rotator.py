import os, time
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.globals import set_llm_cache
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
import requests
import utils

# -------- Clash Verge 控制器 --------
# rotator.py
import re
import time
import requests
import concurrent.futures
from urllib.parse import quote

class ClashVergeController:
    def __init__(self, host="127.0.0.1", port=9097, secret=None,
                 group_preference=("🔰 节点选择", "🚀 手动切换", "GLOBAL"),
                 test_url="http://cp.cloudflare.com/generate_204",
                 delay_timeout_ms=2000, max_workers=16):
        self.api = f"http://{host}:{port}"
        self.headers = {"Authorization": f"Bearer {secret}"} if secret else {}
        self.group_preference = list(group_preference)
        self.test_url = test_url
        self.delay_timeout_ms = delay_timeout_ms
        self.max_workers = max_workers

        # 真实叶子节点的类型白名单（按 Clash/Meta 常见类型）
        self.allowed_types = {
            "Shadowsocks", "ShadowTLS", "Trojan", "VLESS", "VMess",
            "Hysteria", "Hysteria2", "TUIC", "Socks", "HTTP"
        }
        # 名称黑名单：过滤“剩余流量/到期/公告/订阅”等“假节点”
        self.name_blacklist = re.compile(r"(剩余|流量|到期|过期|公告|提示|套餐|订阅|失效|续费)", re.I)

    # ------------ 基础请求 ------------
    def _get_json(self, path, **kwargs):
        r = requests.get(f"{self.api}{path}", headers=self.headers, timeout=8, **kwargs)
        r.raise_for_status()
        return r.json()

    def _put_json(self, path, body):
        r = requests.put(f"{self.api}{path}", json=body, headers=self.headers, timeout=8)
        r.raise_for_status()
        return r

    def _patch_json(self, path, body):
        r = requests.patch(f"{self.api}{path}", json=body, headers=self.headers, timeout=8)
        r.raise_for_status()
        return r

    # ------------ 组/节点 ------------
    def guess_main_group(self):
        data = self._get_json("/proxies")["proxies"]
        groups = {k: v for k, v in data.items() if v["type"] in {"Selector", "URLTest", "Fallback"}}
        for name in self.group_preference:
            if name in groups:
                return name
        for name in groups:
            if any(key in name for key in ["Proxy", "选择", "GLOBAL", "手动", "Select"]):
                return name
        # 兜底：任意一个
        return next(iter(groups.keys()))

    def _detail(self, name: str):
        return self._get_json(f"/proxies/{quote(name, safe='')}")

    def _is_real_leaf(self, name: str, info: dict) -> bool:
        if name in ("DIRECT", "REJECT"):
            return False
        if "all" in info and isinstance(info.get("all"), list):
            return False
        t = str(info.get("type", "")).strip()
        if t not in self.allowed_types:
            return False
        if self.name_blacklist.search(name):
            return False
        return True

    def _build_tree_and_leaves(self, root_group: str):
        parent = {}
        leaves = []
        seen = set()

        def dfs(name):
            if name in seen:
                return
            seen.add(name)
            info = self._detail(name)
            children = info.get("all")
            if not isinstance(children, list):
                if self._is_real_leaf(name, info):
                    leaves.append(name)
                return
            for child in children:
                parent.setdefault(child, name)
                try:
                    ci = self._detail(child)
                    if isinstance(ci.get("all"), list):
                        dfs(child)
                    else:
                        if self._is_real_leaf(child, ci):
                            leaves.append(child)
                except Exception:
                    pass

        dfs(root_group)
        # 去重保持顺序
        leaves = list(dict.fromkeys(leaves))
        return leaves, parent

    def _path_from_root(self, parent_map, root, leaf):
        path = [leaf]
        cur = leaf
        while cur in parent_map and parent_map[cur] != cur:
            p = parent_map[cur]
            path.append(p)
            if p == root:
                break
            cur = p
        path.reverse()
        if not path or path[0] != root:
            raise RuntimeError(f"未能从 {root} 构建到 {leaf} 的有效路径")
        return path

    def _probe_delay(self, name: str):
        try:
            r = requests.get(
                f"{self.api}/proxies/{quote(name, safe='')}/delay",
                headers=self.headers,
                timeout=8,
                params={"url": self.test_url, "timeout": self.delay_timeout_ms}
            )
            if r.ok:
                d = r.json().get("delay", None)
                if isinstance(d, int) and d > 0:
                    return name, d
        except Exception:
            pass
        return name, float("inf")

    def list_group_candidates(self, group_name: str):
        # 返回该组的直接子项（不递归）
        info = self._detail(group_name)
        return list(info.get("all", []) or [])

    def current_selected(self, group_name: str):
        info = self._detail(group_name)
        return info.get("now")

    def switch_group(self, group_name: str, child_name: str):
        # 直接子项切换
        self._put_json(f"/proxies/{quote(group_name, safe='')}", {"name": child_name})

    def switch_along_path(self, path):
        # 逐层切换：path=[组0, 子组/叶1, 子组/叶2, ..., 叶]
        for i in range(len(path) - 1):
            g, c = path[i], path[i + 1]
            self.switch_group(g, c)

    def get_runtime_ports(self):
        cfg = self._get_json("/configs")
        return {
            "mixed-port": cfg.get("mixed-port"),
            "port": cfg.get("port"),
            "socks-port": cfg.get("socks-port"),
        }

    def external_ip(self):
        ports = self.get_runtime_ports()
        candidates = []
        if ports.get("mixed-port"):
            p = ports["mixed-port"]
            candidates.append({"http": f"http://127.0.0.1:{p}", "https": f"http://127.0.0.1:{p}"})
        if ports.get("port") and ports.get("port") != ports.get("mixed-port"):
            p = ports["port"]
            candidates.append({"http": f"http://127.0.0.1:{p}", "https": f"http://127.0.0.1:{p}"})
        if ports.get("socks-port"):
            sp = ports["socks-port"]
            candidates.append({"http": f"socks5h://127.0.0.1:{sp}", "https": f"socks5h://127.0.0.1:{sp}"})

        endpoints = [
            ("https://api.ipify.org", {"format": "text"}),
            ("https://api64.ipify.org", {"format": "text"}),
            ("https://ifconfig.me/ip", None),
            ("https://ifconfig.co/ip", None),
            ("https://ipinfo.io/ip", None),
            ("https://ipv4.icanhazip.com", None),
            ("https://ip.sb", None),
            ("https://www.cloudflare.com/cdn-cgi/trace", None),  # 解析 ip=...
        ]

        for prox in candidates:
            for url, params in endpoints:
                try:
                    resp = requests.get(url, params=params, proxies=prox, timeout=8)
                    text = resp.text.strip()
                    if "cdn-cgi/trace" in url:
                        for line in text.splitlines():
                            if line.startswith("ip="):
                                return line.split("=", 1)[1].strip()
                    if text and len(text) <= 128 and re.match(r"[0-9a-fA-F\.\:]+", text):
                        return text
                except Exception:
                    continue
        return "(获取 IP 失败)"

    # ------------ 高级：自动选优并切换 ------------
    def auto_pick_best(self, root_group: str, country_regex: str | None = None) -> tuple[str, int, list[str]]:
        """
        在 root_group 下递归搜集叶子节点（过滤“假节点”），可选按国家正则再过滤；
        并发测速选最优叶子，构建 root->...->leaf 路径并逐层切换。
        返回: (best_node, best_delay_ms, switch_path)
        """
        leaves, parent = self._build_tree_and_leaves(root_group)
        if country_regex:
            leaves = [n for n in leaves if re.search(country_regex, n, re.I)]
        if not leaves:
            raise RuntimeError(f"组 {root_group} 没有可用叶子节点（可能被过滤为空）。")

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(leaves))) as ex:
            results = list(ex.map(self._probe_delay, leaves))
        results = [(n, d) for n, d in results if d != float("inf")]
        if not results:
            raise RuntimeError("所有候选节点测速失败或不可用。")

        best_node, best_delay = min(results, key=lambda x: x[1])
        path = self._path_from_root(parent, root_group, best_node)
        self.switch_along_path(path)
        return best_node, best_delay, path


# -------- 你的 KeyRotator（保持不变/或用你已有的）--------
class KeyRotator:
    """
    从 key.txt 读取多行 API Key，跳过空行和注释(#, //)，去重保序。
    调用 next_key() 时循环前进（到末尾回到首行），并设置到环境变量 OPENAI_API_KEY。
    """
    def __init__(self, key_file: str = "key.txt", env_file: str | None = None,
                 env_key_name: str = "OPENAI_API_KEY"):
        self.key_path = Path(key_file)
        self.env_file = Path(env_file) if env_file else None
        self.env_key_name = env_key_name
        self._keys: List[str] = []
        self._idx: int = -1
        self._load_keys()
        # 如果尚未设置 key，则先设置一个
        if self._keys:
            self.next_key(loop=True)

    def _load_keys(self):
        if not self.key_path.exists():
            raise FileNotFoundError(f"key file not found: {self.key_path}")
        lines = self.key_path.read_text(encoding="utf-8").splitlines()
        cleaned = []
        seen = set()
        for s in lines:
            s = s.strip()
            if not s or s.startswith("#") or s.startswith("//"):
                continue
            if s not in seen:
                seen.add(s)
                cleaned.append(s)
        if not cleaned:
            raise RuntimeError("key.txt 为空或只有注释。")
        self._keys = cleaned

    @property
    def keys(self) -> List[str]:
        return list(self._keys)

    @property
    def current_key(self) -> str | None:
        if not self._keys or self._idx < 0:
            return None
        return self._keys[self._idx]

    def _set_env(self, k: str):
        # 写入进程环境变量；LangChain 的 OpenAI Chat 默认读取 OPENAI_API_KEY
        os.environ[self.env_key_name] = k
        # 可选：同步 .env（如果你用 python-dotenv 在别处 load）
        if self.env_file:
            try:
                # 简单覆写 .env 中该项，不做复杂解析
                text = self.env_file.read_text(encoding="utf-8") if self.env_file.exists() else ""
                lines = []
                found = False
                for line in text.splitlines():
                    if line.startswith(f"{self.env_key_name}="):
                        lines.append(f"{self.env_key_name}={k}")
                        found = True
                    else:
                        lines.append(line)
                if not found:
                    lines.append(f"{self.env_key_name}={k}")
                self.env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
            except Exception:
                # .env 写失败不影响进程内环境变量
                pass

    def next_key(self, loop: bool = True) -> str:
        """
        前进到下一个 key；若到末尾：
          - loop=True 则回到首个 key（无限循环）
          - loop=False 则抛出 StopIteration
        返回新的 key 字符串。
        """
        if not self._keys:
            raise RuntimeError("没有可用的 key。")
        if self._idx < 0:
            self._idx = 0
        else:
            self._idx += 1
            if self._idx >= len(self._keys):
                if loop:
                    self._idx = 0
                else:
                    self._idx = len(self._keys) - 1
                    raise StopIteration("已到达 key 列表末尾。")
        k = self._keys[self._idx]
        self._set_env(k)
        return k

    def reload(self):
        """
        重新从 key.txt 加载（支持你动态增删 key），保持当前位置若还存在；否则从首个开始。
        """
        old = self.current_key
        self._load_keys()
        if not self._keys:
            raise RuntimeError("key.txt 为空。")
        if old in self._keys:
            self._idx = self._keys.index(old)
        else:
            self._idx = -1
            self.next_key(loop=True)
        return self.current_key
