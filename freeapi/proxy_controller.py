import os

_SELECTOR_PREFER = (
    "Proxies",
    "GLOBAL",
    "PROXY",
    "Proxy",
    "🔰 节点选择",
    "节点选择",
    "🚀 手动切换",
    "手动切换",
)
_SKIP_SELECTOR_NAMES = frozenset({"DIRECT", "REJECT", "COMPATIBLE", "PASS", "REJECT-DROP"})
_BAD_NOW_NAMES = _SKIP_SELECTOR_NAMES | frozenset(
    {"自动选择", "故障转移", "负载均衡", "全球直连", "广告拦截"}
)


class ClashControllerConfig:
    # Clash Verge 默认 external controller 常见为 127.0.0.1:9097
    base_url = "http://127.0.0.1:9097"
    secret = None
    selector = "GLOBAL"

    def __init__(self, base_url="http://127.0.0.1:9097", secret=None, selector="GLOBAL"):
        self.base_url = base_url
        self.secret = secret
        self.selector = selector


class ClashController:
    """
    Clash External Controller API（Clash Verge / Hiddify 等客户端通常都兼容）。
    关键接口（Clash 兼容）：
    - GET   /proxies
    - PATCH /proxies/{selector}   body: {"name": "<node_name>"}
    认证：
    - Header: Authorization: Bearer <secret>
      secret 通常在客户端配置里；也可通过环境变量 CLASH_VERGE_SECRET / CLASH_SECRET 提供。
    """

    def __init__(self, cfg):
        self.cfg = cfg
        if not self.cfg.secret:
            self.cfg.secret = os.getenv("CLASH_VERGE_SECRET") or os.getenv("CLASH_SECRET")

    def headers(self):
        h = {"Content-Type": "application/json"}
        if self.cfg.secret:
            h["Authorization"] = f"Bearer {self.cfg.secret}"
        return h

    def headersAlt(self):
        # 有些 Clash 兼容实现用的是不带 Bearer 的 Authorization
        h = {"Content-Type": "application/json"}
        if self.cfg.secret:
            h["Authorization"] = str(self.cfg.secret)
        return h

    def fetch_proxies(self):
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("切换 IP 需要 requests：`pip install requests`") from e

        url = self.cfg.base_url.rstrip("/") + "/proxies"
        r = requests.get(url, headers=self.headers(), timeout=10)
        if r.status_code == 401 and self.cfg.secret:
            r = requests.get(url, headers=self.headersAlt(), timeout=10)
        r.raise_for_status()
        return r.json()

    def _score_selector(self, name, info):
        if not isinstance(info, dict) or info.get("type") != "Selector":
            return -1
        if name in _SKIP_SELECTOR_NAMES:
            return -1
        nodes = [x for x in (info.get("all") or []) if isinstance(x, str)]
        if len(nodes) < 2:
            return -1
        now = str(info.get("now") or "")
        score = len(nodes)
        if name in _SELECTOR_PREFER:
            score += 1000
        if now and now not in _BAD_NOW_NAMES:
            score += 500
        elif now in _BAD_NOW_NAMES:
            score -= 800
        if name == "GLOBAL":
            score -= 300
        return score

    def pick_selector(self, proxies_data):
        proxies = proxies_data.get("proxies") or {}
        prefer = (self.cfg.selector or "").strip()
        if prefer and self._is_selector(proxies, prefer):
            info = proxies[prefer]
            if self._score_selector(prefer, info) >= 0:
                return prefer
        env_sel = (os.getenv("CLASH_SELECTOR") or "").strip()
        if env_sel and env_sel != prefer and self._is_selector(proxies, env_sel):
            info = proxies[env_sel]
            if self._score_selector(env_sel, info) >= 0:
                return env_sel
        best_name = None
        best_score = -1
        for name, info in proxies.items():
            score = self._score_selector(name, info)
            if score > best_score:
                best_score = score
                best_name = name
        if best_name:
            return best_name
        if prefer and self._is_selector(proxies, prefer):
            return prefer
        raise RuntimeError(
            "找不到可用的 Selector 策略组（可设置 CLASH_SELECTOR，或检查 Clash 是否已加载订阅）"
        )

    @staticmethod
    def _is_selector(proxies, name):
        info = proxies.get(name)
        return isinstance(info, dict) and info.get("type") == "Selector"

    def resolve_selector(self):
        """解析并缓存当前订阅可用的 selector 名（不同订阅组名可能不同）。"""
        data = self.fetch_proxies()
        picked = self.pick_selector(data)
        if picked != self.cfg.selector:
            self.cfg.selector = picked
        return picked

    def get_selector_state(self):
        """
        返回 (now, all_nodes)。
        selector 通常是一个 Selector/Group，例如 GLOBAL/PROXY/节点组名。
        """
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("切换 IP 需要 requests：`pip install requests`") from e

        data = self.fetch_proxies()
        proxies = data.get("proxies") or {}
        sel = proxies.get(self.cfg.selector)
        if not isinstance(sel, dict):
            self.resolve_selector()
            data = self.fetch_proxies()
            proxies = data.get("proxies") or {}
            sel = proxies.get(self.cfg.selector)
        if not isinstance(sel, dict):
            raise RuntimeError(f"找不到 selector={self.cfg.selector!r}（请检查 CLASH_SELECTOR）")

        now = sel.get("now")
        all_nodes = sel.get("all") or []
        if not isinstance(now, str) or not isinstance(all_nodes, list) or not all(isinstance(x, str) for x in all_nodes):
            raise RuntimeError(f"selector={self.cfg.selector!r} 返回结构不符合预期：{sel}")
        return now, list(all_nodes)

    def switch_to(self, node_name):
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("切换 IP 需要 requests：`pip install requests`") from e

        url = self.cfg.base_url.rstrip("/") + f"/proxies/{self.cfg.selector}"
        r = requests.patch(url, headers=self.headers(), json={"name": node_name}, timeout=10)
        if r.status_code == 401 and self.cfg.secret:
            r = requests.patch(url, headers=self.headersAlt(), json={"name": node_name}, timeout=10)
        if r.status_code == 405:
            # 部分 Clash 兼容实现用 PUT 而不是 PATCH
            r = requests.put(url, headers=self.headers(), json={"name": node_name}, timeout=10)
            if r.status_code == 401 and self.cfg.secret:
                r = requests.put(url, headers=self.headersAlt(), json={"name": node_name}, timeout=10)
        r.raise_for_status()

    def switch_next(self):
        now, all_nodes = self.get_selector_state()
        if not all_nodes:
            raise RuntimeError(f"selector={self.cfg.selector!r} 没有可切换的节点列表")
        try:
            i = all_nodes.index(now)
            nxt = all_nodes[(i + 1) % len(all_nodes)]
        except ValueError:
            nxt = all_nodes[0]
        if nxt != now:
            self.switch_to(nxt)
        return nxt


__all__ = ["ClashController", "ClashControllerConfig"]

