"""按模型名解析 freeapi 轮换上限（ip_key_node_limit / daily_limit）。"""

from __future__ import annotations


def _norm_model_name(model: str) -> str:
    return (model or "").strip().lower().replace(" ", "").replace("_", "-")


def rotation_limits_for_model(model: str) -> tuple[int, int]:
    """
    返回 (ip_key_node_limit, daily_limit)。
    - gpt-5.4 等非 mini 的 5.x：5 次后切节点/key
    - 名称含 mini：200 次
    """
    m = _norm_model_name(model)
    if "mini" in m:
        return 200, 200
    if "5.4" in m or m in ("gpt-5.4", "gpt5.4"):
        return 5, 5
    # 其它 gpt-5 全尺寸（非 mini）也按 5 次
    if m.startswith("gpt-5") or m.startswith("gpt5"):
        return 5, 5
    return 200, 200
