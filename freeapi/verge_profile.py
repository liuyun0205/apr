from __future__ import annotations

import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

DEFAULT_VERGE_DATA_DIRS = [
    os.path.expanduser("~/.local/share/io.github.clash-verge-rev.clash-verge-rev"),
    os.path.expanduser("~/.config/clash-verge"),
]

# 默认轮换顺序：subscribe -> 八戒 -> 最萌の云
DEFAULT_SUBSCRIPTION_ORDER = (
    "subscribe",
    "八戒",
    "最萌の云 - CuteCloud",
)

_SELECTOR_PREFER = ("Proxies", "GLOBAL", "🔰 节点选择", "节点选择", "Proxy", "PROXY")


class ClashVergeProfileSwitcher:
    """
    在 Clash Verge Rev 的多个 remote 订阅 profile 之间轮换。
    流程：拉取订阅 -> 写入 profiles/*.yaml -> 补丁端口/controller -> PUT /configs 加载。
    注意：绝不使用 path 为空的 PUT /configs（会弄乱运行中配置）。
    """

    def __init__(
        self,
        *,
        data_dir: Optional[str] = None,
        profile_uids: Optional[List[str]] = None,
        controller_base_url: str = "http://127.0.0.1:9097",
        controller_secret: Optional[str] = None,
        selector: Optional[str] = None,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.data_dir = data_dir or detect_verge_data_dir()
        self.profile_uids = (
            profile_uids
            or profile_uids_from_env()
            or default_subscription_profile_uids(self.data_dir)
        )
        self.controller_base_url = controller_base_url.rstrip("/")
        self.controller_secret = controller_secret or os.getenv("CLASH_VERGE_SECRET") or os.getenv("CLASH_SECRET")
        self._selector_override = selector
        self.log = log or (lambda _msg: None)
        self._profiles_path = os.path.join(self.data_dir, "profiles.yaml")
        self._config_path = os.path.join(self.data_dir, "config.yaml")

    def load_profiles_yaml(self) -> Dict[str, Any]:
        with open(self._profiles_path, "r", encoding="utf-8") as f:
            return parse_profiles_yaml_text(f.read())

    def save_profiles_yaml_current(self, uid: str) -> None:
        with open(self._profiles_path, "r", encoding="utf-8") as f:
            text = f.read()
        if re.search(r"^current:\s*", text, re.MULTILINE):
            text = re.sub(r"^current:\s*.+$", f"current: {uid}", text, count=1, flags=re.MULTILINE)
        else:
            text = f"current: {uid}\n" + text
        tmp = self._profiles_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, self._profiles_path)

    def get_profile_item(self, uid: str) -> Dict[str, Any]:
        for item in self.load_profiles_yaml().get("items") or []:
            if isinstance(item, dict) and str(item.get("uid")) == str(uid):
                return item
        raise RuntimeError(f"profiles.yaml 中找不到 uid={uid!r}")

    def current_uid(self) -> str:
        return str(self.load_profiles_yaml().get("current") or "")

    def switch_next(self) -> Tuple[str, str]:
        uids = self.profile_uids
        if not uids:
            raise RuntimeError("没有可轮换的订阅：请设置 CLASH_PROFILE_UIDS 或在 Verge 中添加 remote profile")
        cur = self.current_uid()
        try:
            i = uids.index(cur)
            nxt = uids[(i + 1) % len(uids)]
        except ValueError:
            nxt = uids[0]
        self.activate(nxt)
        item = self.get_profile_item(nxt)
        name = str(item.get("name") or item.get("uid") or nxt)
        return nxt, name

    def activate(self, uid: str) -> None:
        self.log(f"[subscription] 切换 profile uid={uid}")
        self.fetch_remote_profile(uid)
        runtime_path = self.prepare_runtime_config(uid)
        self.reload_configs(runtime_path)
        self.save_profiles_yaml_current(uid)
        self.log(f"[subscription] 已加载 {runtime_path}")

    def fetch_remote_profile(self, uid: str) -> None:
        item = self.get_profile_item(uid)
        url = str(item.get("url") or "").strip()
        file_name = str(item.get("file") or "").strip()
        if not url or not file_name:
            return
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("拉取订阅需要 requests") from e

        proxy = os.getenv("CLASH_PROXY") or os.getenv("HTTP_PROXY")
        proxies = None
        if proxy:
            proxies = {"http": proxy, "https": proxy}

        self.log(f"[subscription] 更新订阅 {item.get('name') or uid}")
        r = requests.get(url, timeout=60, proxies=proxies)
        r.raise_for_status()
        out_path = os.path.join(self.data_dir, "profiles", file_name)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tmp = out_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(r.content)
        os.replace(tmp, out_path)

    def default_selector_for(self, uid: str) -> str:
        if self._selector_override:
            return self._selector_override
        item = self.get_profile_item(uid)
        names: List[str] = []
        for sel in item.get("selected") or []:
            if isinstance(sel, dict) and sel.get("name"):
                names.append(str(sel["name"]))
        for prefer in _SELECTOR_PREFER:
            if prefer in names:
                return prefer
        if names:
            return names[0]
        return os.getenv("CLASH_SELECTOR") or "Proxies"

    def read_verge_base_config(self) -> Dict[str, Any]:
        if not os.path.isfile(self._config_path):
            return {}
        with open(self._config_path, "r", encoding="utf-8") as f:
            return parse_simple_yaml_kv(f.read())

    def prepare_runtime_config(self, uid: str) -> str:
        item = self.get_profile_item(uid)
        file_name = str(item.get("file") or "").strip()
        if not file_name:
            raise RuntimeError(f"profile {uid} 没有 file 字段")
        src = os.path.join(self.data_dir, "profiles", file_name)
        if not os.path.isfile(src):
            raise RuntimeError(f"订阅文件不存在: {src}")

        with open(src, "r", encoding="utf-8") as f:
            content = f.read()

        base = self.read_verge_base_config()
        content = patch_yaml_runtime_fields(content, base)

        out = os.path.join(self.data_dir, ".apr_runtime_switch.yaml")
        with open(out, "w", encoding="utf-8") as f:
            f.write(content)
        return out

    def reload_configs(self, path: str) -> None:
        if not path or not os.path.isfile(path):
            raise RuntimeError(f"无效的配置路径: {path!r}")
        try:
            import requests  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("重载配置需要 requests") from e

        url = self.controller_base_url + "/configs?force=true"
        headers = {"Content-Type": "application/json"}
        if self.controller_secret:
            headers["Authorization"] = f"Bearer {self.controller_secret}"

        body = {"path": os.path.abspath(path), "payload": ""}
        r = requests.put(url, headers=headers, json=body, timeout=30)
        if r.status_code == 401 and self.controller_secret:
            headers["Authorization"] = str(self.controller_secret)
            r = requests.put(url, headers=headers, json=body, timeout=30)
        r.raise_for_status()
        time.sleep(0.5)


def detect_verge_data_dir() -> str:
    env = (os.getenv("CLASH_VERGE_DATA_DIR") or "").strip()
    if env and os.path.isdir(env):
        return env
    for d in DEFAULT_VERGE_DATA_DIRS:
        if os.path.isfile(os.path.join(d, "profiles.yaml")):
            return d
    raise RuntimeError(
        "找不到 Clash Verge 数据目录。请设置环境变量 CLASH_VERGE_DATA_DIR"
    )


def profile_uids_from_env() -> List[str]:
    raw = (os.getenv("CLASH_PROFILE_UIDS") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def default_subscription_profile_uids(data_dir: str) -> List[str]:
    """按 subscribe -> 八戒 -> 最萌の云 顺序解析 remote profile uid。"""
    path = os.path.join(data_dir, "profiles.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = parse_profiles_yaml_text(f.read())

    by_name: Dict[str, str] = {}
    cute_uid: Optional[str] = None
    for item in data.get("items") or []:
        if not isinstance(item, dict) or item.get("type") != "remote":
            continue
        uid = str(item.get("uid") or "").strip()
        name = str(item.get("name") or "").strip()
        if not uid or not name:
            continue
        by_name[name] = uid
        if "最萌" in name and "云" in name:
            cute_uid = uid

    uids: List[str] = []
    for prefer in DEFAULT_SUBSCRIPTION_ORDER:
        uid = by_name.get(prefer)
        if uid and uid not in uids:
            uids.append(uid)
    if cute_uid and cute_uid not in uids:
        uids.append(cute_uid)
    if uids:
        return uids
    return list_remote_profile_uids(data_dir)


def parse_profiles_yaml_text(text: str) -> Dict[str, Any]:
    cur_m = re.search(r"^current:\s*(\S+)", text, re.MULTILINE)
    current = cur_m.group(1) if cur_m else ""
    items: List[Dict[str, Any]] = []
    for block in re.split(r"\n-\s+uid:\s*", text)[1:]:
        lines = block.splitlines()
        uid = lines[0].strip()
        item: Dict[str, Any] = {"uid": uid}
        for line in lines[1:]:
            m = re.match(r"^\s{2}(\w[\w-]*):\s*(.*)$", line)
            if not m:
                continue
            key, val = m.group(1), m.group(2).strip()
            if key in ("type", "name", "file", "url", "desc", "home"):
                item[key] = val.strip("'\"")
        if item.get("type") == "remote":
            sel_blocks = re.findall(
                r"-\s+name:\s*(.+?)\n\s+now:\s*(.*)",
                block,
                re.DOTALL,
            )
            selected = []
            for name, now in sel_blocks:
                selected.append({"name": name.strip(), "now": now.strip() or None})
            if selected:
                item["selected"] = selected
            items.append(item)
    return {"current": current, "items": items}


def parse_simple_yaml_kv(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for line in text.splitlines():
        if not line or line.strip().startswith("#"):
            continue
        if line.startswith(" ") or line.startswith("\t"):
            continue
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        val = val.strip().strip("'\"")
        if key in (
            "mixed-port",
            "port",
            "socks-port",
            "redir-port",
            "tproxy-port",
        ):
            try:
                out[key] = int(val)
            except ValueError:
                out[key] = val
        elif key == "allow-lan":
            out[key] = val.lower() in ("true", "yes", "1")
        else:
            out[key] = val
    return out


def list_remote_profile_uids(data_dir: str) -> List[str]:
    path = os.path.join(data_dir, "profiles.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = parse_profiles_yaml_text(f.read())
    uids: List[str] = []
    for item in data.get("items") or []:
        if isinstance(item, dict) and item.get("type") == "remote" and item.get("uid"):
            uids.append(str(item["uid"]))
    return uids


def patch_yaml_runtime_fields(content: str, base: Dict[str, Any]) -> str:
    """把 Verge 的端口 / external-controller 写进订阅 YAML，避免加载后本地代理端口错乱。"""
    overrides: Dict[str, Any] = {}
    for key in (
        "mixed-port",
        "port",
        "socks-port",
        "redir-port",
        "tproxy-port",
        "external-controller",
        "secret",
        "allow-lan",
        "mode",
        "log-level",
    ):
        if key in base and base[key] is not None:
            overrides[key] = base[key]

    if not overrides:
        return content

    for key, val in overrides.items():
        if isinstance(val, bool):
            rep = "true" if val else "false"
        elif val is None:
            continue
        else:
            rep = str(val)
            if key == "secret":
                rep = f"'{rep}'" if not (rep.startswith("'") or rep.startswith('"')) else rep

        pat = re.compile(rf"^{re.escape(key)}:\s*.*$", re.MULTILINE)
        if pat.search(content):
            content = pat.sub(f"{key}: {rep}", content, count=1)
        else:
            content = f"{key}: {rep}\n" + content
    return content


__all__ = [
    "ClashVergeProfileSwitcher",
    "DEFAULT_SUBSCRIPTION_ORDER",
    "default_subscription_profile_uids",
    "detect_verge_data_dir",
    "list_remote_profile_uids",
    "profile_uids_from_env",
]
