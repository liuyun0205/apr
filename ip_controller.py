# clash_api.py
# -*- coding: utf-8 -*-
"""
Clash/Clash Verge External Controller 纯函数/类接口
默认：
- External Controller: http://127.0.0.1:9097
- 无 secret；如有可在构造时传入
"""

from __future__ import annotations
from random import Random
from typing import Dict, List, Any, Optional, Tuple
import json
import requests

DEFAULT_CONTROLLER = "http://127.0.0.1:9097"
DEFAULT_TEST_URL = "https://www.gstatic.com/generate_204"
DEFAULT_TIMEOUT = 5  # seconds


class ClashClient:
    """轻量封装的 Clash API 客户端。"""

    def __init__(
        self,
        base: str = DEFAULT_CONTROLLER,
        secret: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.base = base.rstrip("/")
        self.secret = secret
        self.timeout = timeout

    # ---------- low-level ----------
    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.secret:
            # 兼容不同版本
            h["Authorization"] = f"Bearer {self.secret}"
            h["secret"] = self.secret
        return h

    def _req(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base}{path}"
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("headers", self._headers())
        # 访问本地控制端口务必禁用代理，避免自我代理导致 407 等问题
        kwargs.setdefault("proxies", {})
        r = requests.request(method, url, **kwargs)
        r.raise_for_status()
        return r

    # ---------- raw endpoints ----------
    def get_proxies_raw(self) -> Dict[str, Any]:
        return self._req("GET", "/proxies").json()

    def get_configs_raw(self) -> Dict[str, Any]:
        return self._req("GET", "/configs").json()

    def set_configs_raw(self, patch: Dict[str, Any]) -> None:
        self._req("PUT", "/configs", data=json.dumps(patch))

    def set_group_selection(self, group: str, name: str) -> None:
        from requests.utils import quote
        body = {"name": name}
        self._req("PUT", f"/proxies/{quote(group, safe='')}", data=json.dumps(body))

    def test_delay(self, name: str, url: str = DEFAULT_TEST_URL, timeout_ms: int = 3000) -> Dict[str, Any]:
        from requests.utils import quote
        return self._req(
            "GET",
            f"/proxies/{quote(name, safe='')}/delay",
            params={"url": url, "timeout": str(timeout_ms)},
        ).json()

    # ---------- helpers ----------
    def list_groups_and_nodes(self) -> Dict[str, List[str]]:
        """
        返回 组 -> 候选节点列表（仅 Selector/URLTest/Fallback/LoadBalance）
        """
        data = self.get_proxies_raw()
        proxies = data.get("proxies", {})
        res: Dict[str, List[str]] = {}
        for g, v in proxies.items():
            if v.get("type") in ("Selector", "URLTest", "Fallback", "LoadBalance"):
                all_list = v.get("all", [])
                if all_list:
                    res[g] = all_list
        return res

    def current_selection(self) -> Dict[str, str]:
        """
        返回 组 -> 当前选中节点
        """
        data = self.get_proxies_raw()
        proxies = data.get("proxies", {})
        cur: Dict[str, str] = {}
        for g, v in proxies.items():
            if v.get("type") in ("Selector", "URLTest", "Fallback", "LoadBalance"):
                now = v.get("now")
                if now:
                    cur[g] = now
        return cur

    def set_mode(self, mode: str) -> None:
        """
        mode: Rule / Global / Direct（大小写不敏感）
        """
        normalized = mode.capitalize()
        if normalized not in ("Rule", "Global", "Direct"):
            raise ValueError("mode 必须是 Rule/Global/Direct 之一")
        self.set_configs_raw({"mode": normalized})

    def pick_fastest_in_group(
        self,
        group: str,
        candidates: Optional[List[str]] = None,
        test_url: str = DEFAULT_TEST_URL,
        timeout_ms: int = 3000,
    ) -> Tuple[str, int]:
        """
        在某分组中测速并返回(最佳节点名, 延迟ms)。若 candidates 为空，将读取该组的 all。
        """
        groups = self.list_groups_and_nodes()
        if group not in groups:
            raise KeyError(f"未找到分组：{group}")

        cand = candidates or groups[group]
        best_name: Optional[str] = None
        best_ms = 10**9

        for name in cand:
            try:
                r = self.test_delay(name, test_url, timeout_ms)
                d = r.get("delay")
                if isinstance(d, (int, float)) and 0 < d < best_ms:
                    best_name, best_ms = name, int(d)
            except Exception:
                # 单个失败忽略
                pass

        if not best_name:
            raise RuntimeError(f"分组 {group} 未测到可用节点")
        return best_name, best_ms

    def switch_to_fastest(
        self,
        group: str,
        test_url: str = DEFAULT_TEST_URL,
        timeout_ms: int = 3000,
    ) -> Tuple[str, int]:
        """
        在分组内测速并自动切换到最快，返回(节点名, 延迟ms)
        """
        name, delay = self.pick_fastest_in_group(group, None, test_url, timeout_ms)
        self.set_group_selection(group, name)
        return name, delay


# ---------- 额外实用函数（基于 ClashClient）----------
NAME_BLOCKLIST = {
    "最新网址", "最新地址", "剩余流量", "过期时间", "到期时间",
    "有效期", "官网", "公告", "流量", "fastlink.cc", "fastlink",
}

def _is_banner_name(name: str) -> bool:
    n = name.strip().lower()
    for kw in NAME_BLOCKLIST:
        if kw.lower() in n:
            return True
    return False

def switch_group_to_node(
    client: ClashClient, group: str, node_name: str
) -> None:
    """将分组切换到指定节点。"""
    # 可选：校验 node 是否在 group 的 all 列表
    groups = client.list_groups_and_nodes()
    if group in groups and node_name not in groups[group]:
        raise ValueError(f"节点《{node_name}》不在分组《{group}》的候选列表中。")
    client.set_group_selection(group, node_name)


def batch_switch_groups(
    client: ClashClient,
    plan: Dict[str, str],
    strict: bool = True,
) -> Dict[str, bool]:
    """
    批量切换分组：plan 形如 { "🔰 节点选择": "🇭🇰 香港-01", "🇯🇵 日本节点": "JP-02" }
    返回各分组切换结果 True/False。
    strict=True 时若节点不在候选里将抛异常；False 则跳过该项并置 False。
    """
    results: Dict[str, bool] = {}
    groups = client.list_groups_and_nodes()

    for group, node in plan.items():
        try:
            if strict and group in groups and node not in groups[group]:
                raise ValueError(f"节点《{node}》不在分组《{group}》候选列表中。")
            client.set_group_selection(group, node)
            results[group] = True
        except Exception:
            results[group] = False
    return results


def get_overview(client: ClashClient) -> Dict[str, Any]:
    """
    给上层做状态展示：返回 { 'mode': ..., 'current': {组: 当前节点}, 'groups': {组: [候选...] } }
    """
    cfg = client.get_configs_raw()
    cur = client.current_selection()
    groups = client.list_groups_and_nodes()
    return {
        "mode": cfg.get("mode"),
        "current": cur,
        "groups": groups,
    }
# 分组类型（需要逐级PUT）
GROUP_TYPES = {"Selector", "URLTest", "Fallback", "LoadBalance"}

# 真实“叶子”节点的协议类型（根据 Clash 内核返回的 type 过滤）
# 在文件顶部把 ALLOWED_LEAF_TYPES 改成不含 Direct/Reject（可避免误选）
ALLOWED_LEAF_TYPES = {
    "Shadowsocks", "ShadowsocksR", "Vmess", "Trojan",
    "Socks5", "HTTP",
    "Hysteria", "Hysteria2", "TUIC", "WireGuard",
    "ShadowTLS", "Relay",
}

def _is_alive(client: ClashClient, name: str, test_url: str, timeout_ms: int) -> Tuple[bool, Optional[int]]:
    try:
        r = client.test_delay(name, url=test_url, timeout_ms=timeout_ms)
        d = r.get("delay")
        if isinstance(d, (int, float)) and d > 0:
            return True, int(d)
    except Exception:
        pass
    return False, None

def switch_group_to_leaf_random_alive(
    client: ClashClient,
    top_group: str,
    test_url: str = "https://www.bing.com",
    timeout_ms: int = 2500,
    shuffle_seed: Optional[int] = None,
) -> Tuple[str, int, List[str]]:
    """
    从 top_group 递归收集所有“真实叶子”，随机打乱后逐个测速，
    选择第一个“未超时/有 delay”的节点，并沿路径逐级PUT切换。
    返回: (chosen_leaf, delay_ms, old_chain)
    """
    leaves = expand_to_leaves(client, top_group)
    if not leaves:
        raise RuntimeError(f"分组《{top_group}》下没有可用叶子节点。")

    # 当前链路（用于回显）
    old_chain = current_leaf_chain(client, top_group)

    # 总是打乱；有 seed 用固定 RNG，没 seed 用系统熵
    rng = Random(shuffle_seed)
    rng.shuffle(leaves)

    for leaf in leaves:
        ok, delay = _is_alive(client, leaf, test_url, timeout_ms)
        if not ok:
            print("测试网页访问失败")
            continue
        graph = build_graph(client)
        path = find_path_to_leaf(graph, top_group, leaf)
        if not path:
            continue
        switch_by_path(client, path)
        return leaf, delay, old_chain

    raise RuntimeError("未找到可用节点：所有候选均测速超时或不可达。")
def _get(client: ClashClient, path: str) -> Dict[str, Any]:
    # 直接复用你的底层 _req；禁用代理由 _req 处理
    return client._req("GET", path).json()

def get_proxy_info(client: ClashClient, name: str) -> dict:
    from requests.utils import quote
    return _get(client, f"/proxies/{quote(name, safe='')}")

def is_group(node: Dict[str, Any]) -> bool:
    return node.get("type") in GROUP_TYPES

def is_leaf(node: Dict[str, Any]) -> bool:
    return (not is_group(node)) and (node.get("type") in ALLOWED_LEAF_TYPES)

def expand_to_leaves(client: ClashClient, name: str) -> List[str]:
    info = get_proxy_info(client, name)
    if not is_group(info):
        # 既要是“叶子类型”，也不能命中名称黑名单
        return [name] if (is_leaf(info) and not _is_banner_name(name)) else []

    leaves: List[str] = []
    for child in info.get("all", []):
        child_info = get_proxy_info(client, child)
        if is_group(child_info):
            leaves.extend(expand_to_leaves(client, child))
        elif is_leaf(child_info) and not _is_banner_name(child):
            leaves.append(child)

    # 去重保持顺序
    uniq, seen = [], set()
    for x in leaves:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq
def current_leaf_chain(client: ClashClient, group: str) -> List[str]:
    """
    从 group 出发，沿 now 一路下探到叶子的“当前链路”。
    例如：['🔰 节点选择', '🚀 手动切换', '香港负载组', 'HK-02(REAL)']
    """
    chain = [group]
    name = group
    while True:
        info = get_proxy_info(client, name)
        if not is_group(info):
            break
        now = info.get("now")
        if not now:
            break
        chain.append(now)
        name = now
    return chain

# ---------------- 逐级切换所需：构建图、找路径、逐级PUT ----------------

def build_graph(client: ClashClient) -> Dict[str, Any]:
    """一次性取 /proxies，减少请求次数。"""
    return client.get_proxies_raw()

def list_children(graph: Dict[str, Any], name: str) -> List[str]:
    node = graph["proxies"].get(name, {})
    return node.get("all", []) if is_group(node) else []

def find_path_to_leaf(graph: Dict[str, Any], top_group: str, target_leaf: str) -> Optional[List[str]]:
    """
    在“分组图”里找从 top_group 到 target_leaf 的路径（包含两端）。
    target_leaf 必须是叶子（且 type 属于 ALLOWED_LEAF_TYPES）。
    """
    proxies = graph.get("proxies", {})
    if top_group not in proxies or target_leaf not in proxies:
        return None

    if not is_leaf(proxies[target_leaf]):
        return None

    path, visited = [], set()
    def dfs(cur: str) -> bool:
        path.append(cur); visited.add(cur)
        if cur == target_leaf:
            return True
        if is_group(proxies.get(cur, {})):
            for nxt in list_children(graph, cur):
                if nxt not in visited and dfs(nxt):
                    return True
        path.pop()
        return False

    ok = dfs(top_group)
    return path if ok else None

def switch_by_path(client: ClashClient, path: List[str]) -> None:
    """
    按路径逐级 PUT：
    [顶层分组, 中间分组, ..., 叶子] →
      PUT /proxies/顶层 {name: 中间}
      PUT /proxies/中间 {name: 下级}
      ...
    """
    if len(path) < 2:
        return
    for i in range(len(path) - 1):
        group, child = path[i], path[i + 1]
        client.set_group_selection(group, child)

# ---------------- 高级接口：随机/测速选择并“逐级切换” ----------------

def switch_group_to_leaf_any(
    client: ClashClient,
    top_group: str,
    avoid_current: bool = True,
    prefer_keywords: Optional[List[str]] = None,
) -> Tuple[str, List[str]]:
    """
    在 top_group 下递归展开所有叶子节点，随机选一个，**逐级PUT**切换。
    - avoid_current=True：尽量避开当前叶子
    - prefer_keywords：优先选择名称包含任意关键字的叶子（如 ['香港','HK']）
    返回：(chosen_leaf, old_chain)
    """
    # 1) 找到所有可达叶子
    leaves = expand_to_leaves(client, top_group)
    if not leaves:
        raise RuntimeError(f"分组《{top_group}》下没有可用叶子节点。")

    # 2) 当前叶子（沿 now 下钻）
    old_chain = current_leaf_chain(client, top_group)
    old_leaf = old_chain[-1] if old_chain else None

    # 3) 关键字优先
    cand = leaves
    if prefer_keywords:
        pri = [n for n in leaves if any(k in n for k in prefer_keywords)]
        if pri:
            cand = pri

    # 4) 避开当前
    if avoid_current and old_leaf in cand and len(cand) > 1:
        cand = [n for n in cand if n != old_leaf]

    # 5) 随机挑选，并**逐级PUT**到该叶子
    rnd=Random()
    chosen = rnd.choice(cand)
    graph = build_graph(client)
    path = find_path_to_leaf(graph, top_group, chosen)
    if not path:
        raise RuntimeError(f"找不到从《{top_group}》到《{chosen}》的路径，或目标不是叶子。")
    switch_by_path(client, path)
    return chosen, old_chain

def switch_group_to_leaf_fastest(
    client: ClashClient,
    top_group: str,
    test_url: str = "https://www.gstatic.com/generate_204",
    timeout_ms: int = 3000,
    prefer_keywords: Optional[List[str]] = None,
) -> Tuple[str, int, List[str]]:
    """
    在 top_group 下递归展开所有叶子节点，测速选最快，**逐级PUT**切换。
    返回：(chosen_leaf, delay_ms, old_chain)
    """
    leaves = expand_to_leaves(client, top_group)
    if not leaves:
        raise RuntimeError(f"分组《{top_group}》下没有可用叶子节点。")

    if prefer_keywords:
        pri = [n for n in leaves if any(k in n for k in prefer_keywords)]
        if pri:
            leaves = pri

    best, best_ms = None, 10**9
    for n in leaves:
        try:
            r = client.test_delay(n, url=test_url, timeout_ms=timeout_ms)
            d = r.get("delay")
            if isinstance(d, (int, float)) and 0 < d < best_ms:
                best, best_ms = n, int(d)
        except Exception:
            pass
    if not best:
        raise RuntimeError("所有候选叶子测速失败。")

    old_chain = current_leaf_chain(client, top_group)
    graph = build_graph(client)
    path = find_path_to_leaf(graph, top_group, best)
    if not path:
        raise RuntimeError(f"找不到从《{top_group}》到《{best}》的路径。")
    switch_by_path(client, path)
    return best, best_ms, old_chain


# ------------------- 演示用（你可保留或移除） -------------------
def switch_ip():
    client = ClashClient(base="http://127.0.0.1:9097", secret=None)
    TOP = "🔰 节点选择"
    # 切换前链路
    before = current_leaf_chain(client, TOP)
    print("切换前:", " -> ".join(before))

    # 随机选择一个测速不超时的真实节点，并逐级 PUT 切换过去
    leaf, delay, old = switch_group_to_leaf_random_alive(
        client, TOP,
        test_url="https://api.chatanywhere.org/",
        timeout_ms=3000,  # 如果网络波动，建议 3000~5000
    )
    print(f"已切换到：{leaf}（{delay} ms）；原链：{' -> '.join(old)}")

    # 切换后链路
    after = current_leaf_chain(client, TOP)
    print("切换后:", " -> ".join(after))
if __name__ == "__main__":
    switch_ip()