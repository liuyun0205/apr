import json
import os
from datetime import datetime

from freeapi.proxy_controller import ClashController, ClashControllerConfig


def todayYmd():
    return datetime.now().strftime("%Y-%m-%d")


def maskKey(k):
    if len(k) <= 10:
        return k[:2] + "***"
    return f"{k[:6]}...{k[-4:]}"


_STATE_META = "_meta"


class IPRingConfig:
    # “同一天、同一个 key、同一个节点”允许的最大次数；超过就切下一个节点
    per_key_per_node_limit = 200

    # Clash controller（Clash Verge 常见端口 9097）
    controller_base_url = "http://127.0.0.1:9097"
    controller_secret = None
    selector = "GLOBAL"

    # 当前订阅有 k 个节点时，累计切换满 subscription_rotate_cycles * k 次后换下一个订阅（默认 1 轮）
    subscription_rotate_cycles = 1

    # 轮换到下一个 Clash Verge remote profile
    enable_subscription_rotation = True
    clash_profile_uids = None  # 逗号分隔 uid 列表；为空则自动读取 profiles.yaml 里所有 remote
    clash_verge_data_dir = None

    # 状态文件（sidecar），默认放在 key.csv 同目录
    state_path = None

    def __init__(
        self,
        per_key_per_node_limit=200,
        controller_base_url="http://127.0.0.1:9097",
        controller_secret=None,
        selector="GLOBAL",
        subscription_rotate_cycles=1,
        enable_subscription_rotation=True,
        clash_profile_uids=None,
        clash_verge_data_dir=None,
        state_path=None,
    ):
        self.per_key_per_node_limit = per_key_per_node_limit
        self.controller_base_url = controller_base_url
        self.controller_secret = controller_secret
        self.selector = selector
        self.subscription_rotate_cycles = int(subscription_rotate_cycles or 1)
        self.enable_subscription_rotation = enable_subscription_rotation
        self.clash_profile_uids = clash_profile_uids
        self.clash_verge_data_dir = clash_verge_data_dir
        self.state_path = state_path


class IPRing:
    """
    维护 “key + node” 的当日计数，并在超过阈值时通过 Clash API 切换节点（例如 Clash Verge）。
    """

    def __init__(self, cfg, *, key_csv_path, log=None):
        self.cfg = cfg
        self.log = log or (lambda _msg: None)
        if self.cfg.state_path is None:
            self.cfg.state_path = key_csv_path + ".ipring.json"

        self.ctrl = ClashController(
            ClashControllerConfig(
                base_url=self.cfg.controller_base_url,
                secret=self.cfg.controller_secret,
                selector=self.cfg.selector,
            )
        )
        try:
            picked = self.ctrl.resolve_selector()
            if picked != self.cfg.selector:
                self.log(f"[clash] 自动选择 selector={picked!r}")
                self.cfg.selector = picked
        except Exception as e:
            self.log(f"[clash] selector 自动检测失败（忽略）: {repr(e)}")

        self.profile_switcher = None
        if self.cfg.enable_subscription_rotation:
            from freeapi.verge_profile import (
                ClashVergeProfileSwitcher,
                default_subscription_profile_uids,
                detect_verge_data_dir,
                profile_uids_from_env,
            )

            uids = None
            if self.cfg.clash_profile_uids:
                uids = [x.strip() for x in str(self.cfg.clash_profile_uids).split(",") if x.strip()]
            elif profile_uids_from_env():
                uids = profile_uids_from_env()
            else:
                data_dir = self.cfg.clash_verge_data_dir or detect_verge_data_dir()
                uids = default_subscription_profile_uids(data_dir)

            self.profile_switcher = ClashVergeProfileSwitcher(
                data_dir=self.cfg.clash_verge_data_dir,
                profile_uids=uids,
                controller_base_url=self.cfg.controller_base_url,
                controller_secret=self.cfg.controller_secret,
                selector=self.cfg.selector,
                log=self.log,
            )

    def loadState(self):
        p = self.cfg.state_path or ""
        if not p or not os.path.exists(p):
            return {}
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}

    def saveState(self, data):
        p = self.cfg.state_path or ""
        tmp = p + ".tmp"
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, p)

    def getCount(self, st, *, day, key, node):
        return int(((st.get(day) or {}).get(key) or {}).get(node) or 0)

    def incCount(self, st, *, day, key, node, inc=1):
        st.setdefault(day, {})
        st[day].setdefault(key, {})
        st[day][key][node] = int(st[day][key].get(node) or 0) + int(inc)

    def get_node_switch_count(self, st, *, day):
        return int(((st.get(day) or {}).get(_STATE_META) or {}).get("node_switches") or 0)

    def inc_node_switch_count(self, st, *, day, inc=1):
        st.setdefault(day, {})
        st[day].setdefault(_STATE_META, {})
        st[day][_STATE_META]["node_switches"] = self.get_node_switch_count(st, day=day) + int(inc)

    def reset_node_switch_count(self, st, *, day):
        st.setdefault(day, {})
        st[day].setdefault(_STATE_META, {})
        st[day][_STATE_META]["node_switches"] = 0

    def should_rotate_subscription(self, st, *, day):
        """当前订阅节点数为 k 时，当日切换次数 >= cycles * k 则换订阅。"""
        _, all_nodes = self.ctrl.get_selector_state()
        k = len(all_nodes)
        if k <= 0:
            return False
        cycles = max(1, int(self.cfg.subscription_rotate_cycles or 1))
        return self.get_node_switch_count(st, day=day) >= cycles * k

    def switch_next_tracked(self, *, key):
        """
        切到下一个节点并累计当日切换次数；满 1 轮（默认 k 次）后换订阅。
        返回是否已切换订阅。
        """
        day = todayYmd()
        st = self.loadState()
        self.ctrl.switch_next()
        self.inc_node_switch_count(st, day)
        switches = self.get_node_switch_count(st, day)
        _, all_nodes = self.ctrl.get_selector_state()
        k = len(all_nodes)
        cycles = max(1, int(self.cfg.subscription_rotate_cycles or 1))
        if k > 0 and switches >= cycles * k:
            sub_switched, _ = self.maybeSwitchSubscription(st, day=day, key=key)
            return sub_switched
        self.saveState(st)
        return False

    def maybeSwitchSubscription(self, st, *, day, key):
        if not self.profile_switcher:
            return False, ""
        try:
            self.ctrl.resolve_selector()
            self.cfg.selector = self.ctrl.cfg.selector
        except Exception as e:
            self.log(f"[clash] 换订阅前 selector 检测失败: {repr(e)}")
            return False, ""
        if not self.should_rotate_subscription(st, day=day):
            return False, ""
        switches = self.get_node_switch_count(st, day=day)
        _, all_nodes = self.ctrl.get_selector_state()
        k = len(all_nodes)
        cycles = max(1, int(self.cfg.subscription_rotate_cycles or 1))
        uid, name = self.profile_switcher.switch_next()
        self.ctrl.cfg.selector = self.profile_switcher.default_selector_for(uid)
        try:
            picked = self.ctrl.resolve_selector()
            self.cfg.selector = picked
        except Exception as e:
            self.log(f"[clash] 换订阅后 selector 检测失败: {repr(e)}")
        st.setdefault(day, {})
        st[day][key] = {}
        self.reset_node_switch_count(st, day=day)
        self.saveState(st)
        self.log(
            f"[rotate] subscription -> {name} (uid={uid}, "
            f"node_switches={switches}>={cycles}*{k}, selector={self.ctrl.cfg.selector})"
        )
        return True, name

    def ensure_ok_before_request(self, *, key):
        """
        返回 (before_node, after_node, count_on_after_node, switched)。
        如果达到阈值，会先切到下一个节点。
        """
        day = todayYmd()
        before, _ = self.ctrl.get_selector_state()
        st = self.loadState()
        cnt_before = self.getCount(st, day=day, key=key, node=before)
        switched = False
        after = before
        if cnt_before >= self.cfg.per_key_per_node_limit:
            self.switch_next_tracked(key=key)
            switched = True
            after, _ = self.ctrl.get_selector_state()
            st = self.loadState()
        cnt_after = self.getCount(st, day=day, key=key, node=after)
        return before, after, cnt_after, switched

    def record_success(self, *, key):
        day = todayYmd()
        now, _ = self.ctrl.get_selector_state()
        st = self.loadState()
        self.incCount(st, day=day, key=key, node=now, inc=1)
        self.saveState(st)

    def mark_current_node_exhausted(self, st, *, day, key, node):
        """把当前节点对该 key 的计数记满，避免下次仍选到同一节点。"""
        st.setdefault(day, {})
        st[day].setdefault(key, {})
        st[day][key][node] = self.cfg.per_key_per_node_limit

    def on_quota_likely_ip(self, *, key):
        """
        429 且 key 本地未达日限时：视为 IP/节点超限。
        记满当前节点计数并切下一个节点（必要时换订阅）。
        """
        day = todayYmd()
        st = self.loadState()
        before, _ = self.ctrl.get_selector_state()
        self.mark_current_node_exhausted(st, day=day, key=key, node=before)
        self.saveState(st)
        self.switch_next_tracked(key=key)

    def on_quota_or_429(self, *, key):
        """网络错误等场景：切节点，并把当前节点记满。"""
        self.on_quota_likely_ip(key=key)


__all__ = ["IPRing", "IPRingConfig"]

_today_ymd = todayYmd
_mask_key = maskKey

