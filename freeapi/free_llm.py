import os
import time


def subscription_rotation_enabled(cfg) -> bool:
    if os.getenv("CLASH_DISABLE_SUBSCRIPTION_ROTATION", "").lower() in ("1", "true", "yes"):
        return False
    if os.getenv("CLASH_ENABLE_SUBSCRIPTION_ROTATION", "").lower() in ("0", "false", "no"):
        return False
    return bool(getattr(cfg, "enable_subscription_rotation", True))


class FreeLLMConfig:
    """
    专门用于“freeapi 轮换策略”的 LLM 配置：
    - key 轮换 / Clash 代理 / Clash 节点切换 / Host1&2
    这些策略不放在 LLM.py 里，而放在本模块中。
    """

    def __init__(
        self,
        model,
        system_prompt="",
        forward_host1="https://api.chatanywhere.tech",
        forward_host2="https://api.chatanywhere.org",
        key_csv="/home/liuzhihao/文档/key.csv",
        daily_limit=200,
        clash_proxy="http://127.0.0.1:7897",
        enable_ip_rotation=True,
        # 同一个 key 在同一个节点上最多使用 N 次，达到阈值后切换下一个节点/订阅
        ip_key_node_limit=200,
        clash_controller="http://127.0.0.1:9097",
        clash_secret="930087",  # Clash Verge external controller secret
        clash_selector="Proxies",
        enable_subscription_rotation=True,
        clash_profile_uids="R6PHYpuG17Lb,RVfww9LmhIsF,R8kFeWvVbfE1",
        clash_verge_data_dir="",
        # ipipgo：从 API 拉代理列表，遇到错误就切下一个代理
        ipipgo_api_url="",
        ipipgo_upstream_proxy="",  # 如访问 ipipgo API 需要走本地 clash：http://127.0.0.1:7897
        ipipgo_refresh_every=50,  # 每 N 次请求刷新一次代理列表
        ipipgo_tunnel_hostport="",  # 例如 proxy.ipipgo.com:31212（优先于 ipipgo_api_url）
    ):
        self.model = model
        self.system_prompt = system_prompt
        self.forward_host1 = forward_host1
        self.forward_host2 = forward_host2
        self.key_csv = key_csv
        self.daily_limit = daily_limit
        self.clash_proxy = clash_proxy
        self.enable_ip_rotation = enable_ip_rotation
        self.ip_key_node_limit = ip_key_node_limit
        self.clash_controller = clash_controller
        self.clash_secret = clash_secret
        self.clash_selector = clash_selector
        self.enable_subscription_rotation = enable_subscription_rotation
        self.clash_profile_uids = clash_profile_uids
        self.clash_verge_data_dir = clash_verge_data_dir
        self.ipipgo_api_url = ipipgo_api_url
        self.ipipgo_upstream_proxy = ipipgo_upstream_proxy
        self.ipipgo_refresh_every = ipipgo_refresh_every
        self.ipipgo_tunnel_hostport = ipipgo_tunnel_hostport

class FreeLLM:
    def __init__(self, cfg):
        self.cfg = cfg

        # 初始化 keyring / ipring
        from freeapi.keyring import Keyring  # type: ignore

        self.keyring = Keyring(self.keyCsvPath(), daily_limit=self.cfg.daily_limit)

        self.ipring = None
        self._ipipgo = None
        if self.cfg.enable_ip_rotation:
            # 优先使用 ipipgo 隧道代理（你的 curl -x ... -U ... 这种）
            tunnel = os.getenv("IPIPGO_TUNNEL_HOSTPORT") or self.cfg.ipipgo_tunnel_hostport
            if tunnel:
                self._ipipgo = _IPIPGOTunnelProxy(
                    hostport=tunnel,
                    log=self.log,
                )
            else:
                # 次选：ipipgo API 拉取 ip:port 列表（但通常这些节点需要单独的认证账号）
                ipipgo_api_url = os.getenv("IPIPGO_API_URL") or self.cfg.ipipgo_api_url
                if ipipgo_api_url:
                    self._ipipgo = _IPIPGOProxyRing(
                        api_url=ipipgo_api_url,
                        upstream_proxy=os.getenv("IPIPGO_UPSTREAM_PROXY") or self.cfg.ipipgo_upstream_proxy or (os.getenv("CLASH_PROXY") or self.cfg.clash_proxy),
                        refresh_every=int(os.getenv("IPIPGO_REFRESH_EVERY") or self.cfg.ipipgo_refresh_every),
                        log=self.log,
                    )
                else:
                    try:
                        from freeapi.ipring import IPRing, IPRingConfig  # type: ignore

                        sub_on = subscription_rotation_enabled(self.cfg)
                        self.ipring = IPRing(
                            IPRingConfig(
                                per_key_per_node_limit=self.cfg.ip_key_node_limit,
                                controller_base_url=self.clashController(),
                                controller_secret=self.clashSecret(),
                                selector=self.clashSelector(),
                                enable_subscription_rotation=sub_on,
                                clash_profile_uids=os.getenv("CLASH_PROFILE_UIDS")
                                or self.cfg.clash_profile_uids
                                or None,
                                clash_verge_data_dir=os.getenv("CLASH_VERGE_DATA_DIR")
                                or self.cfg.clash_verge_data_dir
                                or None,
                            ),
                            key_csv_path=self.keyCsvPath(),
                            log=self.log,
                        )
                    except Exception:
                        self.ipring = None

        # 轮换 key 时：默认走 host2；代理策略：
        # - 如果启用 ipipgo：请求时显式传 proxies，不污染全局环境变量
        # - 否则沿用原逻辑：走 CLASH_PROXY 环境变量
        if self._ipipgo is None:
            proxy = os.getenv("CLASH_PROXY") or self.cfg.clash_proxy
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
            os.environ["ALL_PROXY"] = proxy

    def keyCsvPath(self):
        return os.getenv("FREEAPI_KEY_CSV") or self.cfg.key_csv

    def clashController(self):
        return os.getenv("CLASH_CONTROLLER") or self.cfg.clash_controller

    def clashSecret(self):
        return os.getenv("CLASH_VERGE_SECRET") or os.getenv("CLASH_SECRET") or self.cfg.clash_secret

    def clashSelector(self):
        return os.getenv("CLASH_SELECTOR") or self.cfg.clash_selector

    def log(self, msg):
        try:
            from tqdm import tqdm  # type: ignore

            tqdm.write(msg)
        except Exception:
            print(msg, flush=True)

    def forwardHost1(self):
        return os.getenv("FREEAPI_FORWARD_HOST1") or self.cfg.forward_host1

    def forwardHost2(self):
        return os.getenv("FREEAPI_FORWARD_HOST2") or self.cfg.forward_host2

    def baseUrlForRequest(self):
        # 优先级：
        # 1) FREEAPI_BASE_URL：完全自定义
        # 2) FREEAPI_USE_HOST1=1：强制走 host1（国内）
        # 3) 默认走 host2（国外/轮换）
        forced = (os.getenv("FREEAPI_BASE_URL") or "").strip()
        if forced:
            return forced
        use_host1 = (os.getenv("FREEAPI_USE_HOST1") or "").strip().lower() in ("1", "true", "yes", "y", "on")
        if use_host1:
            return self.forwardHost1()
        return self.forwardHost2()

    def chat(self, user_content, *, system_prompt=None):
        sys_prompt = self.cfg.system_prompt if system_prompt is None else system_prompt

        messages = []
        if sys_prompt.strip():
            messages.append({"role": "system", "content": sys_prompt})
        messages.append({"role": "user", "content": user_content})

        base_url = self.baseUrlForRequest()
        url = base_url.rstrip("/") + "/v1/chat/completions"

        payload = {
            "model": self.cfg.model,
            "messages": messages,
        }

        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("freeapi 模式需要 requests：`pip install requests`") from e

        last_key = None
        last_err = None

        def formatRespBody(resp, limit=2000):
            try:
                txt = resp.text or ""
            except Exception:
                txt = ""
            txt = txt.strip()
            if not txt:
                return ""
            if len(txt) > limit:
                return txt[:limit] + "\n...<truncated>..."
            return txt

        # 换 key 上限：最多尝试 200 次，避免无限循环
        for _ in range(200):
            api_key = self.keyring.acquire()
            if api_key != last_key:
                try:
                    from freeapi.keyring import maskKey  # type: ignore

                    self.log(f"[rotate] key {maskKey(last_key) if last_key else 'None'} -> {maskKey(api_key)}")
                except Exception:
                    pass
                last_key = api_key

            # 同一 key 上：429 先只换 IP；换过 IP 仍 429 再认定 key 真超限
            ip_rotated_on_429 = False

            # 可能触发阈值切节点（Clash）或切下一个代理（ipipgo）
            if self._ipipgo is not None:
                self._ipipgo.ensure_ready()
            elif self.ipring is not None:
                try:
                    before_node, after_node, _, switched = self.ipring.ensure_ok_before_request(key=api_key)
                    if switched:
                        self.log(f"[rotate] ip/node {before_node} -> {after_node} (threshold)")
                except Exception as e:
                    # IP 不用管报错：尽量切到下一个节点；切换也失败就忽略
                    self.log(f"[rotate] ip/node switch error (ignored): {repr(e)}")
                    try:
                        b, _ = self.ipring.ctrl.get_selector_state()
                    except Exception:
                        b = "unknown"
                    try:
                        self.ipring.switch_next_tracked(key=api_key)
                    except Exception as ee:
                        self.log(f"[rotate] ip/node switch_next error (ignored): {repr(ee)}")
                    try:
                        a, _ = self.ipring.ctrl.get_selector_state()
                    except Exception:
                        a = "unknown"
                    if a != b:
                        self.log(f"[rotate] ip/node {b} -> {a} (forced)")

            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

            # 针对网络/TLS 抖动做 3 次重试；失败时尝试切节点再重试同 key
            for net_try in range(3):
                try:
                    proxies = None
                    if self._ipipgo is not None:
                        proxies = self._ipipgo.current_requests_proxies()
                    # ipipgo 代理通常质量参差：单次不要等太久；超时后快速换下一个代理/刷新列表
                    req_timeout = 30 if self._ipipgo is not None else 120
                    r = requests.post(url, headers=headers, json=payload, timeout=req_timeout, proxies=proxies)
                    # 不管成功还是失败，只要有 body 就打印（方便定位错误信息）
                    body_preview = formatRespBody(r)
                    if body_preview:
                        self.log(f"[http] status={r.status_code} body:\n{body_preview}")

                    try:
                        data = r.json() if r.content else {}
                    except Exception:
                        data = {}
                    if r.status_code >= 400:
                        # prompt 过长（免费接口限制 4096 tokens）：不可恢复，直接交给上层做 skip
                        if r.status_code == 403:
                            txt = (r.text or "")
                            if ("4096" in txt) and ("token" in txt.lower() or "Token" in txt or "token小于" in txt):
                                raise RuntimeError("FREEAPI_PROMPT_TOO_LONG_4096")
                        # quota/429：key 本地未超限 → 先当 IP 超限只换 IP；换过仍 429 → key 真超限
                        if r.status_code == 429:
                            if (
                                not ip_rotated_on_429
                                and self.keyring.is_below_daily_limit(api_key)
                            ):
                                ip_rotated_on_429 = True
                                if self._ipipgo is not None:
                                    self._ipipgo.rotate(reason="quota/429 (ip)")
                                elif self.ipring is not None:
                                    try:
                                        b, _ = self.ipring.ctrl.get_selector_state()
                                    except Exception:
                                        b = "unknown"
                                    try:
                                        self.ipring.on_quota_likely_ip(key=api_key)
                                    except Exception as ee:
                                        self.log(f"[rotate] ip/node switch error (ignored): {repr(ee)}")
                                    try:
                                        a, _ = self.ipring.ctrl.get_selector_state()
                                    except Exception:
                                        a = "unknown"
                                    self.log(
                                        f"[rotate] ip/node {b} -> {a} (quota/429, key below limit, ip only)"
                                    )
                                else:
                                    self.log("[rotate] quota/429: key below limit, no ip rotation configured")
                                continue
                            self.keyring.mark_exhausted(api_key)
                            try:
                                from freeapi.keyring import maskKey  # type: ignore

                                mk = maskKey(api_key)
                            except Exception:
                                mk = "?"
                            self.log(
                                f"[rotate] key {mk} marked exhausted "
                                f"(quota/429 after ip rotate or key at limit)"
                            )
                            break
                        r.raise_for_status()

                    # 成功才计数
                    self.keyring.record_success(api_key)
                    if self.ipring is not None:
                        self.ipring.record_success(key=api_key)
                    return data["choices"][0]["message"]["content"]
                except Exception as e:
                    last_err = e
                    # requests 的异常里如果带 response，也把内容打出来
                    resp = getattr(e, "response", None)
                    if resp is not None:
                        try:
                            body_preview = formatRespBody(resp)
                            if body_preview:
                                self.log(f"[http] error_response status={resp.status_code} body:\n{body_preview}")
                        except Exception:
                            pass
                    if self._ipipgo is not None:
                        self._ipipgo.rotate(reason="net error")
                    elif self.ipring is not None:
                        try:
                            b, _ = self.ipring.ctrl.get_selector_state()
                        except Exception:
                            b = "unknown"
                        try:
                            self.ipring.on_quota_or_429(key=api_key)
                        except Exception as ee:
                            self.log(f"[rotate] ip/node switch error (ignored): {repr(ee)}")
                        try:
                            a, _ = self.ipring.ctrl.get_selector_state()
                        except Exception:
                            a = "unknown"
                        if a != b:
                            self.log(f"[rotate] ip/node {b} -> {a} (net error)")
                    if net_try == 2:
                        break
                    continue

        raise RuntimeError("freeapi: exhausted all keys / retries") from last_err


class _IPIPGOProxyRing:
    def __init__(self, *, api_url: str, upstream_proxy: str, refresh_every: int, log):
        self.api_url = api_url
        self.upstream_proxy = upstream_proxy
        self.refresh_every = max(1, int(refresh_every or 1))
        self.log = log
        self._proxies = []
        self._idx = 0
        self._req_count = 0
        # 如果一批代理“挨个都不行”，就直接拉新的一批，避免在同一批里空转太久
        self._batch_tried = 0
        self._batch_started_at = 0.0

    def _auth(self):
        # 某些 ipipgo “代理列表”节点本身需要账号密码认证（407）。
        u = os.getenv("IPIPGO_PROXY_USERNAME") or os.getenv("IPIPGO_USERNAME") or ""
        p = os.getenv("IPIPGO_PROXY_PASSWORD") or os.getenv("IPIPGO_PASSWORD") or ""
        if u and p:
            return u, p
        return None

    def _with_auth(self, hostport: str) -> str:
        auth = self._auth()
        if not auth:
            return hostport
        u, p = auth
        # requests 的 proxy URL 需要 URL-encode
        try:
            from urllib.parse import quote
        except Exception:
            return hostport
        uu = quote(u, safe="")
        pp = quote(p, safe="")
        if hostport.startswith("http://") or hostport.startswith("https://"):
            scheme, rest = hostport.split("://", 1)
            return f"{scheme}://{uu}:{pp}@{rest}"
        return f"{uu}:{pp}@{hostport}"

    def _fetch(self):
        try:
            from ipipgo import fetch_proxies  # type: ignore
        except Exception as e:
            raise RuntimeError("找不到 ipipgo.py 或 fetch_proxies()，请确认项目根目录存在 ipipgo.py") from e

        items = fetch_proxies(api_url=self.api_url, upstream_proxy=self.upstream_proxy)
        self._proxies = list(items or [])
        self._idx = 0
        self._batch_tried = 0
        self._batch_started_at = time.monotonic()
        if self._proxies:
            self.log(f"[rotate] ipipgo proxies refreshed, count={len(self._proxies)}")
        else:
            self.log("[rotate] ipipgo proxies refreshed, but got empty list")

    def ensure_ready(self):
        self._req_count += 1
        if not self._proxies or (self._req_count % self.refresh_every == 1):
            self._fetch()

    def current(self):
        if not self._proxies:
            return None
        self._idx = int(self._idx) % len(self._proxies)
        return self._proxies[self._idx]

    def current_requests_proxies(self):
        p = self.current()
        if not p:
            return None
        p = self._with_auth(p)
        if not (p.startswith("http://") or p.startswith("https://")):
            p = "http://" + p
        return {"http": p, "https": p}

    def rotate(self, *, reason: str):
        if not self._proxies:
            self._fetch()
            return
        before = self.current() or "none"
        self._idx = (self._idx + 1) % len(self._proxies)
        after = self.current() or "none"
        self._batch_tried += 1

        # 规则：当这批代理基本都试过了（或在这批上耗时超过 30s）且仍在失败轮换时，直接换新的一批
        # 触发点放在 rotate()：free_llm 在网络错误/超时时会调用 rotate()
        if self._proxies:
            batch_age = time.monotonic() - (self._batch_started_at or time.monotonic())
            if self._batch_tried >= len(self._proxies) or batch_age >= 30:
                self.log(f"[rotate] ipipgo batch exhausted/slow (tried={self._batch_tried}/{len(self._proxies)}, age={batch_age:.1f}s), refreshing...")
                self._fetch()
                return

        if after != before:
            self.log(f"[rotate] ipipgo proxy {before} -> {after} ({reason})")


class _IPIPGOTunnelProxy:
    """
    ipipgo 隧道代理：proxy.ipipgo.com:31212 + 账号密码。
    轮换方式：如果 IPIPGO_USERNAME 包含 `{session}`，则在 rotate() 时递增 session 值。
    """

    def __init__(self, *, hostport: str, log):
        self.hostport = hostport
        self.log = log
        self._session = 1

    def ensure_ready(self):
        return

    def _creds(self):
        u = os.getenv("IPIPGO_USERNAME") or os.getenv("IPIPGO_PROXY_USERNAME") or ""
        p = os.getenv("IPIPGO_PASSWORD") or os.getenv("IPIPGO_PROXY_PASSWORD") or ""
        if not u or not p:
            raise RuntimeError("缺少 IPIPGO_USERNAME/IPIPGO_PASSWORD（隧道代理需要账号密码）")
        if "{session}" in u:
            u = u.format(session=self._session)
        return u, p

    def current_requests_proxies(self):
        try:
            from ipipgo import build_proxy_url  # type: ignore
        except Exception:
            # fallback：不做 encode
            u, p = self._creds()
            proxy = f"http://{u}:{p}@{self.hostport}"
            return {"http": proxy, "https": proxy}

        u, p = self._creds()
        proxy = build_proxy_url(hostport=self.hostport, username=u, password=p)
        return {"http": proxy, "https": proxy}

    def rotate(self, *, reason: str):
        before = self._session
        self._session += 1
        if self._session != before:
            self.log(f"[rotate] ipipgo tunnel session {before} -> {self._session} ({reason})")


__all__ = ["FreeLLM", "FreeLLMConfig", "subscription_rotation_enabled"]

# backward-compat aliases
FreeLLM._key_csv_path = FreeLLM.keyCsvPath  # type: ignore[attr-defined]
FreeLLM._clash_controller = FreeLLM.clashController  # type: ignore[attr-defined]
FreeLLM._clash_secret = FreeLLM.clashSecret  # type: ignore[attr-defined]
FreeLLM._clash_selector = FreeLLM.clashSelector  # type: ignore[attr-defined]
FreeLLM._log = FreeLLM.log  # type: ignore[attr-defined]

