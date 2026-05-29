from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import requests


def _parse_text_hostports(text: str) -> list[str]:
    items: list[str] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ":" not in ln:
            continue
        host, port = ln.rsplit(":", 1)
        if not host or not port.isdigit():
            continue
        items.append(f"{host}:{port}")
    return items


def fetch_proxies(*, api_url: str, upstream_proxy: str = "", timeout: int = 8) -> list[str]:
    """
    给 freeapi/free_llm.py 使用：
    - 返回值：["ip:port", ...]
    - 兼容 ipipgo 的 format=text / format=json
    - upstream_proxy：如果拉代理列表本身需要走本地 Clash，就传 http://127.0.0.1:7897
    """
    proxies = None
    if upstream_proxy:
        p = upstream_proxy.strip()
        if not (p.startswith("http://") or p.startswith("https://")):
            p = "http://" + p
        proxies = {"http": p, "https": p}

    resp = requests.get(api_url, timeout=timeout, proxies=proxies)
    resp.raise_for_status()

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ct:
        data = resp.json()
        if isinstance(data, dict) and data.get("code") not in (0, "0", None):
            raise RuntimeError(f"ipipgo API 失败: {data}")
        arr = data.get("data") if isinstance(data, dict) else None
        out: list[str] = []
        if isinstance(arr, list):
            for it in arr:
                if isinstance(it, dict) and it.get("host") and it.get("port"):
                    out.append(f"{it['host']}:{it['port']}")
                elif isinstance(it, str):
                    out.extend(_parse_text_hostports(it))
        if out:
            return out
        # json 但结构不符合时，兜底按文本切
        return _parse_text_hostports(resp.text)

    return _parse_text_hostports(resp.text)


def build_proxy_url(*, hostport: str, username: str, password: str, scheme: str = "http") -> str:
    # requests 的 proxy URL 需要 URL-encode
    from urllib.parse import quote

    uu = quote(username or "", safe="")
    pp = quote(password or "", safe="")
    hp = hostport.strip()
    if hp.startswith("http://") or hp.startswith("https://"):
        # 如果传进来已经带 scheme，就保留它
        scheme, rest = hp.split("://", 1)
        return f"{scheme}://{uu}:{pp}@{rest}"
    return f"{scheme}://{uu}:{pp}@{hp}"


@dataclass(frozen=True)
class ProxyCheckResult:
    proxy: str
    ok: bool
    status_code: Optional[int] = None
    body_preview: str = ""
    error: str = ""


def _check_proxy(proxy: str, *, target_url: str, timeout: int = 4) -> ProxyCheckResult:
    px = proxy
    if not (px.startswith("http://") or px.startswith("https://")):
        px = "http://" + px
    try:
        r = requests.get(target_url, proxies={"http": px, "https": px}, timeout=timeout)
        return ProxyCheckResult(
            proxy=proxy,
            ok=(200 <= r.status_code < 400),
            status_code=r.status_code,
            body_preview=(r.text or "")[:200],
        )
    except Exception as e:
        return ProxyCheckResult(proxy=proxy, ok=False, error=repr(e))


def main() -> None:
    # 仅用于手动跑通：验证 ipipgo 拉到的代理是否可用
    api_url = "https://proxyapi.horocn.com/api/v2/proxies?order_id=LVRO1849729266099302&num=20&format=text&line_separator=win&can_repeat=yes&user_token=46eb8488048c6559a6b03cd7141df990"
    target_url = "http://42.194.246.108:9444/ip"

    hostports = fetch_proxies(api_url=api_url)
    if not hostports:
        raise RuntimeError("没有拉到任何代理")

    for hp in hostports:
        res = _check_proxy(hp, target_url=target_url)
        if res.ok:
            print(f"{hp} => {res.status_code}, {res.body_preview}")
        else:
            print(f"{hp} 失败: {res.error}")


if __name__ == "__main__":
    main()