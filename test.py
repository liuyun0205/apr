#!/usr/bin/env python3
"""
根据 prompt/solver.txt 生成代码，run_solve 跑测例，并与官方 output 对比。

示例（APPS 第 0 题 + 第 0 条测例）:
  CUDA_VISIBLE_DEVICES=0 python test.py \\
    --model_path ~/lzh/Qwen2.5-Coder-7B-Instruct \\
    --dataset_path ~/lzh/datasets/APPS/train --idx 0 --input_idx 0

跑一题全部测例:
  python test.py --code_file out.py \\
    --dataset_path ~/lzh/datasets/APPS/train --idx 0 --all_cases

Agreement 与 Hidden Pass 相关性分析:
  python test.py --analyze_agreement \\
    --agreement_jsonl outputs/agreement_probe.jsonl \\
    --output_dir outputs/agreement_analysis

采集 CURE（Agreement 用 hidden 前 m 条，hidden_pass 仍用全套）:
  CUDA_VISIBLE_DEVICES=1 python test.py --collect_agreement_probe \\
    --dataset cure_codecontests --dataset_path ~/datasets/CURE_codecontests \\
    --agreement_source hidden --probe_count 5 --start 0 --end 1000 ...

采集 LiveCodeBench（eval=public+private hidden tests）:
  CUDA_VISIBLE_DEVICES=1 python test.py --collect_agreement_probe \\
    --dataset livecodebench --dataset_path ~/lzh/datasets/LiveCodeBench \\
    --exec_workers 32 ...
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "model"))

import utils

_PLACEHOLDER_MODEL_PATHS = frozenset({"...", "...", "/path/to/model", "<model_path>"})

_DATASET_CHOICES = [
    "apps",
    "codecontests",
    "cure_codecontests",
    "codecontestplus",
    "livecodebench",
    "codeforces",
]

_AGREEMENT_BUCKETS: List[Tuple[str, float, float]] = [
    ("[0.00,0.25)", 0.00, 0.25),
    ("[0.25,0.50)", 0.25, 0.50),
    ("[0.50,0.75)", 0.50, 0.75),
    ("[0.75,1.00]", 0.75, 1.00 + 1e-12),
]


def _get_agent():
    from agent import Agent

    return Agent


def _load_dataset(dataset: str, path: str, **kwargs):
    from alldatasets.loader import load_dataset

    return load_dataset(dataset, path, **kwargs)


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _pearson_corr(xs: Sequence[float], ys: Sequence[float]) -> float:
    n = len(xs)
    if n < 2 or n != len(ys):
        return float("nan")
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x == 0.0 or den_y == 0.0:
        return float("nan")
    return num / (den_x * den_y)


def _rankdata(values: Sequence[float]) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda t: t[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def _spearman_corr(xs: Sequence[float], ys: Sequence[float]) -> float:
    if len(xs) < 2 or len(xs) != len(ys):
        return float("nan")
    return _pearson_corr(_rankdata(xs), _rankdata(ys))


def _proportion_z_test_one_sided(
    success_high: int,
    n_high: int,
    success_low: int,
    n_low: int,
) -> Tuple[float, float]:
    """检验 P_high > P_low（单侧）。"""
    if n_high <= 0 or n_low <= 0:
        return float("nan"), float("nan")
    p_high = success_high / n_high
    p_low = success_low / n_low
    p_pool = (success_high + success_low) / (n_high + n_low)
    se = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / n_high + 1.0 / n_low))
    if se == 0.0:
        if p_high > p_low:
            return float("inf"), 0.0
        if p_high < p_low:
            return float("-inf"), 1.0
        return 0.0, 0.5
    z = (p_high - p_low) / se
    p_value = 1.0 - _norm_cdf(z)
    return z, p_value


def _linear_trend(xs: Sequence[float], ys: Sequence[float]) -> Tuple[float, float]:
    """返回 (slope, intercept)。"""
    n = len(xs)
    if n < 2:
        return 0.0, (ys[0] if ys else 0.0)
    mx = sum(xs) / n
    my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0.0:
        return 0.0, my
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
    intercept = my - slope * mx
    return slope, intercept


def _bucket_label(agreement: float) -> str:
    for label, lo, hi in _AGREEMENT_BUCKETS:
        if lo <= agreement < hi:
            return label
    return _AGREEMENT_BUCKETS[-1][0]


def compute_agreement(solver_outputs: Sequence[str], naive_outputs: Sequence[Sequence[str]]) -> float:
    """
    A_i = (1 / (N_n * m)) Σ_n Σ_j 1[solver_out(j) == naive_out_n(j)]
    """
    m = len(solver_outputs)
    if m == 0 or not naive_outputs:
        return 0.0

    matches = 0
    total = 0
    for naive_row in naive_outputs:
        if len(naive_row) != m:
            raise ValueError(
                f"naive_outputs 行长度 {len(naive_row)} != solver_outputs 长度 {m}"
            )
        for j in range(m):
            total += 1
            if _normalize_output(solver_outputs[j]) == _normalize_output(naive_row[j]):
                matches += 1
    return matches / total if total else 0.0


def load_agreement_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    p = Path(path).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"找不到 jsonl: {p}")
    with p.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            required = ("hidden_pass", "solver_outputs", "naive_outputs")
            missing = [k for k in required if k not in obj]
            if missing:
                raise ValueError(f"{p}:{line_no} 缺少字段: {missing}")
            records.append(obj)
    if not records:
        raise ValueError(f"jsonl 为空: {p}")
    return records


def analyze_agreement_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for rec in records:
        solver_outputs = [str(x) for x in rec["solver_outputs"]]
        naive_outputs = [[str(x) for x in row] for row in rec["naive_outputs"]]
        agreement = compute_agreement(solver_outputs, naive_outputs)
        hidden_pass = int(rec["hidden_pass"])
        rows.append(
            {
                "problem_id": rec.get("problem_id", ""),
                "solver_id": rec.get("solver_id", len(rows)),
                "hidden_pass": hidden_pass,
                "agreement": agreement,
                "bucket": _bucket_label(agreement),
                "m": len(solver_outputs),
                "n_naive": len(naive_outputs),
            }
        )

    agreements = [r["agreement"] for r in rows]
    ground_truth = [float(r["hidden_pass"]) for r in rows]
    pearson = _pearson_corr(agreements, ground_truth)
    spearman = _spearman_corr(agreements, ground_truth)

    bucket_stats: List[Dict[str, Any]] = []
    for label, lo, hi in _AGREEMENT_BUCKETS:
        group = [r for r in rows if lo <= r["agreement"] < hi]
        count = len(group)
        passed = sum(r["hidden_pass"] for r in group)
        pass_rate = passed / count if count else float("nan")
        bucket_stats.append(
            {
                "bucket": label,
                "count": count,
                "pass_rate": pass_rate,
            }
        )

    high_group = [r for r in rows if r["agreement"] >= 0.75]
    low_group = [r for r in rows if r["agreement"] < 0.75]
    n_high = len(high_group)
    n_low = len(low_group)
    pass_high = sum(r["hidden_pass"] for r in high_group)
    pass_low = sum(r["hidden_pass"] for r in low_group)
    p_high = pass_high / n_high if n_high else float("nan")
    p_low = pass_low / n_low if n_low else float("nan")
    delta_a = math.log((p_high + 1e-6) / (p_low + 1e-6))
    z_score, p_value = _proportion_z_test_one_sided(pass_high, n_high, pass_low, n_low)

    useful = (
        (not math.isnan(pearson))
        and pearson > 0.2
        and delta_a > 0.0
        and (not math.isnan(p_value))
        and p_value < 0.05
    )
    conclusion = (
        "Agreement is a useful correctness signal."
        if useful
        else "Agreement does not appear to predict correctness."
    )

    return {
        "n_records": len(rows),
        "rows": rows,
        "pearson": pearson,
        "spearman": spearman,
        "bucket_stats": bucket_stats,
        "p_high": p_high,
        "p_low": p_low,
        "n_high": n_high,
        "n_low": n_low,
        "pass_high": pass_high,
        "pass_low": pass_low,
        "delta_a": delta_a,
        "z_score": z_score,
        "p_value": p_value,
        "conclusion": conclusion,
    }


def _print_agreement_report(result: Dict[str, Any]) -> None:
    print("\n=== Agreement Analysis ===")
    print(f"n_records: {result['n_records']}")
    print(f"Pearson corr(A, G):  {result['pearson']:.6f}")
    print(f"Spearman corr(A, G): {result['spearman']:.6f}")

    print("\nAgreement Bucket | Count | Hidden Pass Rate")
    print("-" * 48)
    for item in result["bucket_stats"]:
        rate = item["pass_rate"]
        rate_str = f"{rate:.4f}" if not math.isnan(rate) else "nan"
        print(f"{item['bucket']:<16} | {item['count']:<5} | {rate_str}")

    print("\nHigh vs Low Agreement")
    print(f"P_high = P(G=1 | A>=0.75) = {result['p_high']:.6f}  (n={result['n_high']})")
    print(f"P_low  = P(G=1 | A<0.75)  = {result['p_low']:.6f}  (n={result['n_low']})")
    print(f"Delta_A = log((P_high+1e-6)/(P_low+1e-6)) = {result['delta_a']:.6f}")
    print(f"z-score = {result['z_score']:.6f}")
    print(f"p-value (one-sided, P_high > P_low) = {result['p_value']:.6g}")
    print(f"\nConclusion: {result['conclusion']}")


def _save_agreement_plots(result: Dict[str, Any], output_dir: Path) -> List[Path]:
    try:
        import matplotlib.pyplot as plt
    except ImportError as e:
        raise RuntimeError("绘图需要 matplotlib：pip install matplotlib") from e

    output_dir.mkdir(parents=True, exist_ok=True)
    rows = result["rows"]
    xs = [r["agreement"] for r in rows]
    ys = [float(r["hidden_pass"]) for r in rows]

    saved: List[Path] = []

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(xs, ys, alpha=0.45, s=24, edgecolors="none")
    if len(xs) >= 2:
        slope, intercept = _linear_trend(xs, ys)
        x_line = [min(xs), max(xs)]
        y_line = [slope * x + intercept for x in x_line]
        ax.plot(x_line, y_line, color="crimson", linewidth=2, label="trend line")
        ax.legend()
    ax.set_xlabel("Agreement")
    ax.set_ylabel("HiddenPass")
    ax.set_title("Agreement vs Hidden Pass")
    ax.set_ylim(-0.05, 1.05)
    ax.grid(alpha=0.25)
    scatter_path = output_dir / "agreement_scatter.png"
    fig.tight_layout()
    fig.savefig(scatter_path, dpi=160)
    plt.close(fig)
    saved.append(scatter_path)

    buckets = result["bucket_stats"]
    labels = [b["bucket"] for b in buckets]
    rates = [
        0.0 if math.isnan(b["pass_rate"]) else b["pass_rate"]
        for b in buckets
    ]
    counts = [b["count"] for b in buckets]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(labels, rates, color="#4C78A8")
    ax.set_xlabel("Agreement Bucket")
    ax.set_ylabel("Hidden Pass Rate")
    ax.set_title("Agreement Bucket vs Pass Rate")
    ax.set_ylim(0.0, 1.05)
    ax.grid(axis="y", alpha=0.25)
    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.02,
            f"n={count}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    bucket_path = output_dir / "agreement_bucket.png"
    fig.tight_layout()
    fig.savefig(bucket_path, dpi=160)
    plt.close(fig)
    saved.append(bucket_path)

    return saved


def run_agreement_analysis(jsonl_path: str, output_dir: str) -> Dict[str, Any]:
    records = load_agreement_jsonl(jsonl_path)
    result = analyze_agreement_records(records)
    out = Path(output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)

    report = {
        "input_jsonl": str(Path(jsonl_path).expanduser()),
        "n_records": result["n_records"],
        "pearson": result["pearson"],
        "spearman": result["spearman"],
        "bucket_stats": result["bucket_stats"],
        "p_high": result["p_high"],
        "p_low": result["p_low"],
        "n_high": result["n_high"],
        "n_low": result["n_low"],
        "pass_high": result["pass_high"],
        "pass_low": result["pass_low"],
        "delta_a": result["delta_a"],
        "z_score": result["z_score"],
        "p_value": result["p_value"],
        "conclusion": result["conclusion"],
    }
    (out / "agreement_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out / "agreement_rows.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in result["rows"]) + "\n",
        encoding="utf-8",
    )

    plot_paths = _save_agreement_plots(result, out)
    _print_agreement_report(result)
    print("\nSaved:")
    print(f"  {out / 'agreement_report.json'}")
    print(f"  {out / 'agreement_rows.jsonl'}")
    for p in plot_paths:
        print(f"  {p}")
    return result


def _apply_gpu_env(gpu: str) -> None:
    spec = (gpu or "").strip()
    if spec:
        os.environ["CUDA_VISIBLE_DEVICES"] = spec


def _resolve_lora_path(lora: str) -> Optional[str]:
    path = (lora or "").strip()
    if not path:
        return None
    lp = Path(path).expanduser()
    if not (lp / "adapter_config.json").is_file():
        raise SystemExit(f"LoRA 目录无效（缺少 adapter_config.json）: {lp}")
    return str(lp)


def _build_probe_llm(
    model_path: str,
    *,
    max_new_tokens: int,
    temperature: float,
    tensor_parallel_size: int,
    gpu_memory_utilization: float,
    lora_path: Optional[str] = None,
    max_lora_rank: int = 64,
):
    from LLM import LLM, LLMConfig

    return LLM(
        LLMConfig(
            model_type="local",
            model=model_path,
            model_path=model_path,
            system_prompt="",
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            tensor_parallel_size=max(1, int(tensor_parallel_size)),
            gpu_memory_utilization=float(gpu_memory_utilization),
            enable_lora=bool(lora_path),
            max_lora_rank=max(1, int(max_lora_rank)),
        )
    )


def _chat_batch_retry(
    llm,
    questions: Sequence[str],
    *,
    system_prompt: str,
    retries: int,
    sleep_s: float,
    lora_path: Optional[str] = None,
) -> List[str]:
    if not questions:
        return []
    last_err: Optional[Exception] = None
    for attempt in range(max(1, retries + 1)):
        try:
            return [
                t.strip()
                for t in llm.chat_batch(
                    list(questions),
                    system_prompt=system_prompt,
                    lora_path=lora_path,
                )
            ]
        except Exception as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(sleep_s)
    raise RuntimeError(f"LLM 调用失败: {last_err!r}")


def _generate_probe_inputs(
    llm,
    question: str,
    *,
    testcase_prompt: str,
    probe_count: int,
    timeout: int,
    retries: int,
    sleep_s: float,
    fallback_inputs: Sequence[str],
) -> List[str]:
    """生成 m 条 probe stdin；失败时回退到数据集 public inputs。"""
    need = max(1, int(probe_count))
    inputs: List[str] = []

    try:
        raw = _chat_batch_retry(
            llm,
            [question],
            system_prompt=testcase_prompt,
            retries=retries,
            sleep_s=sleep_s,
        )[0]
        gen_code = utils.clean_code(raw)
        if gen_code.strip():
            max_attempts = max(need * 4, need)
            attempts = 0
            while len(inputs) < need and attempts < max_attempts:
                attempts += 1
                stdout, stderr = utils.run_code(gen_code, timeout=timeout)
                if (stderr or "").strip():
                    continue
                for case in utils.parse_trigger_stdout(stdout):
                    text = (case or "").strip()
                    if text:
                        inputs.append(text)
                    if len(inputs) >= need:
                        break
    except Exception as e:
        print(f"[probe] generator 失败，回退 public inputs: {e}")

    if len(inputs) < need:
        for inp in fallback_inputs:
            text = (inp or "").strip()
            if text and text not in inputs:
                inputs.append(text)
            if len(inputs) >= need:
                break

    return inputs[:need]


def _run_outputs_on_inputs(
    code: str,
    inputs: Sequence[str],
    *,
    timeout: int,
) -> List[str]:
    code = utils.clean_code(code)
    outs: List[str] = []
    for inp in inputs:
        stdout, stderr = utils.run_solve_plain(code, inp, timeout=timeout)
        if utils.run_solve_ok(stderr):
            outs.append(_normalize_output(stdout))
        else:
            outs.append(f"__ERROR__:{stderr or 'runtime_error'}")
    return outs


def _probe_code_outputs_worker(payload: Tuple[str, List[str], int]) -> List[str]:
    code, inputs, timeout = payload
    return _run_outputs_on_inputs(code, inputs, timeout=timeout)


def _hidden_pass_worker(payload: Tuple[str, List[str], List[str], int]) -> int:
    code, inputs, outputs, timeout = payload
    return int(
        utils.solver_passes_all_cases(
            code,
            inputs,
            outputs,
            timeout=timeout,
        )
    )


def _parallel_run_codes_on_probe(
    codes: Sequence[str],
    probe_inputs: Sequence[str],
    *,
    timeout: int,
    workers: int,
) -> List[List[str]]:
    if not codes:
        return []
    n_workers = max(1, min(int(workers), len(codes)))
    if n_workers <= 1:
        return [
            _run_outputs_on_inputs(code, probe_inputs, timeout=timeout)
            for code in codes
        ]

    tasks = [(code, list(probe_inputs), timeout) for code in codes]
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        return list(pool.map(_probe_code_outputs_worker, tasks))


def _parallel_hidden_pass(
    solver_codes: Sequence[str],
    hidden_inputs: Sequence[str],
    hidden_outputs: Sequence[str],
    *,
    timeout: int,
    workers: int,
) -> List[int]:
    if not solver_codes:
        return []
    n_workers = max(1, min(int(workers), len(solver_codes)))
    if n_workers <= 1:
        return [
            int(
                utils.solver_passes_all_cases(
                    code,
                    hidden_inputs,
                    hidden_outputs,
                    timeout=timeout,
                )
            )
            for code in solver_codes
        ]

    tasks = [
        (code, list(hidden_inputs), list(hidden_outputs), timeout)
        for code in solver_codes
    ]
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        return list(pool.map(_hidden_pass_worker, tasks))


def _eval_io_label(dataset_name: str) -> str:
    name = (dataset_name or "").strip().lower()
    if name in ("livecodebench", "lcb"):
        return "public+private hidden tests"
    if name in ("cure_codecontests", "cure_cc"):
        return "test_input/test_output"
    return "dataset hidden tests"


def _load_completed_problem_ids(
    jsonl_path: Path,
    *,
    num_solver: int,
) -> Set[str]:
    counts: Dict[str, int] = {}
    if not jsonl_path.is_file():
        return set()
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = str(obj.get("problem_id", ""))
            if pid:
                counts[pid] = counts.get(pid, 0) + 1
    return {pid for pid, n in counts.items() if n >= num_solver}


def _append_jsonl_records(path: Path, records: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _load_probe_dataset(args: argparse.Namespace):
    from alldatasets.loader import default_dataset_path

    name = (args.dataset or "cure_codecontests").strip().lower()
    if name == "apps" and not (args.dataset_path or "").strip():
        name = "cure_codecontests"

    path = (args.dataset_path or "").strip() or default_dataset_path(name)
    extra: Dict[str, Any] = {}

    if name in ("cure_codecontests", "cure_cc"):
        extra["split"] = (args.dataset_split or "train").strip().lower()
        extra["rollout_io_source"] = "tests"
        extra["public_io_source"] = "sample"
    elif name in ("livecodebench", "lcb"):
        extra["include_public"] = True
        extra["include_private"] = True

    ds = _load_dataset(name, path, **extra)
    return ds, path, name


def _resolve_agreement_inputs(
    args: argparse.Namespace,
    *,
    llm,
    question: str,
    hidden_inputs: Sequence[str],
    testcase_prompt: str,
    probe_count: int,
    timeout: int,
    fallback_inputs: Sequence[str],
) -> Tuple[List[str], str]:
    """返回 (agreement 用 inputs, source_tag)。"""
    source = (args.agreement_source or "probe").strip().lower()
    need = max(1, int(probe_count))

    if source == "hidden":
        if not hidden_inputs:
            return [], "hidden_slice"
        return list(hidden_inputs[:need]), "hidden_slice"

    if source != "probe":
        raise SystemExit(f"未知 --agreement_source: {args.agreement_source!r}")

    inputs = _generate_probe_inputs(
        llm,
        question,
        testcase_prompt=testcase_prompt,
        probe_count=need,
        timeout=timeout,
        retries=args.retry,
        sleep_s=args.retry_sleep,
        fallback_inputs=fallback_inputs,
    )
    return inputs, "probe"


def _public_fallback_inputs(ds, idx: int, max_count: int) -> List[str]:
    getter = getattr(ds, "get_public_io_inputs", None)
    if not callable(getter):
        return []
    inputs = list(getter(idx, max_count=max_count if max_count > 0 else 0))
    if max_count > 0:
        return inputs[:max_count]
    return inputs


def collect_agreement_probe(args: argparse.Namespace) -> None:
    """采集 Agreement 实验 jsonl（兼容 cure_codecontests / livecodebench）。"""
    model_dir = _validate_model_path(args.model_path)
    out_path = Path(args.agreement_jsonl).expanduser()
    num_solver = max(1, int(args.num_solver))
    num_naive = max(1, int(args.num_naive))
    probe_count = max(1, int(args.probe_count))
    timeout = int(args.timeout)

    ds, ds_path, dataset_name = _load_probe_dataset(args)
    eval_io = _eval_io_label(dataset_name)
    exec_workers = max(1, int(args.exec_workers))
    agreement_src = (args.agreement_source or "probe").strip().lower()
    print(
        f"[dataset] {dataset_name} path={ds_path} "
        f"split={getattr(ds, 'split', '')} "
        f"eval_io={eval_io} agreement_source={agreement_src} "
        f"agreement_m={probe_count} exec_workers={exec_workers}"
    )
    solver_prompt = utils.file2text(args.prompt_file)
    naive_prompt = utils.file2text(args.naive_prompt_file)
    testcase_prompt = utils.file2text(args.testcase_prompt_file)
    solver_lora = _resolve_lora_path(getattr(args, "solver_lora", ""))
    naive_lora = _resolve_lora_path(getattr(args, "naive_lora", ""))

    end = args.end if args.end is not None else len(ds.df)
    indices = list(range(args.start, end))
    if args.max_items > 0:
        indices = indices[: args.max_items]

    done_ids: Set[str] = set()
    if args.resume:
        done_ids = _load_completed_problem_ids(out_path, num_solver=num_solver)
        if done_ids:
            print(f"[resume] 已完成 {len(done_ids)} 题，跳过")

    llm = _build_probe_llm(
        str(model_dir),
        max_new_tokens=args.max_new_tokens,
        temperature=args.temperature,
        tensor_parallel_size=args.tensor_parallel_size,
        gpu_memory_utilization=args.gpu_memory_utilization,
        lora_path=solver_lora or naive_lora,
        max_lora_rank=args.max_lora_rank,
    )

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    iterator = indices
    if tqdm is not None:
        iterator = tqdm(indices, desc="collect_agreement_probe", unit="problem")

    written_problems = 0
    skipped = 0
    failed = 0

    for idx in iterator:
        pid = str(ds.get_by_tag("id", idx))
        if pid in done_ids:
            skipped += 1
            continue

        question = ds.get_by_tag("description", idx)
        hidden_inputs = ds.get_io_inputs(idx, max_count=0)
        hidden_outputs = ds.get_io_outputs(idx, max_count=0)
        if not hidden_inputs or len(hidden_inputs) != len(hidden_outputs):
            print(f"[skip] idx={idx} id={pid}: 无评测测例 ({eval_io})")
            failed += 1
            continue

        fallback = _public_fallback_inputs(ds, idx, max_count=probe_count)
        agreement_inputs, agreement_input_source = _resolve_agreement_inputs(
            args,
            llm=llm,
            question=question,
            hidden_inputs=hidden_inputs,
            testcase_prompt=testcase_prompt,
            probe_count=probe_count,
            timeout=timeout,
            fallback_inputs=fallback,
        )
        if not agreement_inputs:
            print(f"[skip] idx={idx} id={pid}: 无 agreement inputs")
            failed += 1
            continue
        if len(agreement_inputs) < probe_count:
            print(
                f"[warn] idx={idx} id={pid}: agreement_inputs={len(agreement_inputs)} "
                f"< {probe_count} ({agreement_input_source})"
            )

        try:
            solver_raw = _chat_batch_retry(
                llm,
                [question] * num_solver,
                system_prompt=solver_prompt,
                retries=args.retry,
                sleep_s=args.retry_sleep,
                lora_path=solver_lora,
            )
            naive_raw = _chat_batch_retry(
                llm,
                [question] * num_naive,
                system_prompt=naive_prompt,
                retries=args.retry,
                sleep_s=args.retry_sleep,
                lora_path=naive_lora,
            )
        except Exception as e:
            print(f"[fail] idx={idx} id={pid}: codegen 失败: {e}")
            failed += 1
            continue

        solver_codes = [utils.clean_code(t) for t in solver_raw]
        naive_codes = [utils.clean_code(t) for t in naive_raw]
        if not all(solver_codes) or not all(naive_codes):
            print(f"[fail] idx={idx} id={pid}: 存在空代码")
            failed += 1
            continue

        naive_outputs = []
        solver_probe_outputs: List[List[str]] = []
        all_codes = naive_codes + solver_codes
        all_probe_outputs = _parallel_run_codes_on_probe(
            all_codes,
            agreement_inputs,
            timeout=timeout,
            workers=exec_workers,
        )
        naive_outputs = all_probe_outputs[:num_naive]
        solver_probe_outputs = all_probe_outputs[num_naive:]

        hidden_passes = _parallel_hidden_pass(
            solver_codes,
            hidden_inputs,
            hidden_outputs,
            timeout=timeout,
            workers=min(exec_workers, num_solver),
        )

        records: List[Dict[str, Any]] = []
        for solver_id, solver_code in enumerate(solver_codes):
            hidden_pass = hidden_passes[solver_id]
            solver_outputs = solver_probe_outputs[solver_id]
            records.append(
                {
                    "problem_id": pid,
                    "problem_idx": idx,
                    "solver_id": solver_id,
                    "hidden_pass": hidden_pass,
                    "solver_outputs": solver_outputs,
                    "naive_outputs": naive_outputs,
                    "probe_inputs": agreement_inputs,
                    "agreement_source": agreement_input_source,
                    "m": len(agreement_inputs),
                    "n_naive": len(naive_outputs),
                    "dataset": dataset_name,
                    "dataset_path": ds_path,
                    "dataset_split": getattr(ds, "split", ""),
                    "eval_io_source": eval_io,
                }
            )

        _append_jsonl_records(out_path, records)
        written_problems += 1
        if tqdm is not None and hasattr(iterator, "set_postfix"):
            iterator.set_postfix(written=written_problems, skip=skipped, fail=failed)

    print(
        f"\n[done] 写入 {written_problems} 题 → {out_path} "
        f"(skip={skipped}, fail={failed}, 每题 {num_solver} 条 solver 记录)"
    )


def _validate_model_path(model_path: str) -> Path:
    raw = (model_path or "").strip()
    if raw in _PLACEHOLDER_MODEL_PATHS or "..." in raw:
        raise SystemExit(
            "无效的 --model_path（不能把文档占位符 '...' 当路径）。\n"
            "请改成真实目录，例如：\n"
            "  --model_path ~/lzh/Qwen2.5-Coder-7B-Instruct"
        )
    p = Path(raw).expanduser()
    if not p.is_dir():
        raise SystemExit(f"模型目录不存在: {p}")
    if not (p / "config.json").exists():
        raise SystemExit(
            f"目录下缺少 config.json，不像 HuggingFace 模型: {p}\n"
            "请确认 --model_path 指向已下载的模型根目录。"
        )
    return p


def _normalize_output(text: str) -> str:
    return (text or "").replace("\r\n", "\n").strip()


def _outputs_match(actual: str, expected: str) -> bool:
    return _normalize_output(actual) == _normalize_output(expected)


def _load_io_pair(args, case_idx: int) -> tuple[str, str | None]:
    """返回 (input_str, expected_out|None)。"""
    if args.input is not None and case_idx > 0:
        raise SystemExit("手写 --input 时不能用 --all_cases / case_idx>0")

    if args.input is not None:
        raw = args.input
        if raw.startswith("@"):
            inp = Path(raw[1:]).expanduser().read_text(encoding="utf-8")
        else:
            inp = raw.replace("\\n", "\n")
        exp = _load_expected_manual(args)
        return inp, exp

    if args.dataset_path is not None and args.idx is not None:
        ds = _load_dataset(args.dataset, args.dataset_path)
        inputs = ds.get_io_inputs(args.idx, max_count=0)
        outputs = ds.get_io_outputs(args.idx, max_count=0)
        if not inputs:
            raise SystemExit(f"idx={args.idx} 无 input_output 测例")
        if case_idx >= len(inputs):
            raise SystemExit(
                f"case {case_idx} 超出 inputs 范围（共 {len(inputs)} 条）"
            )
        inp = inputs[case_idx]
        exp = outputs[case_idx] if case_idx < len(outputs) else None
        return inp, exp

    raise SystemExit("请指定 --input / --dataset_path + --idx")


def _load_expected_manual(args) -> str | None:
    if args.expected is not None:
        raw = args.expected
        if raw.startswith("@"):
            return Path(raw[1:]).expanduser().read_text(encoding="utf-8")
        return raw.replace("\\n", "\n")
    if args.expected_file:
        return Path(args.expected_file).expanduser().read_text(encoding="utf-8")
    return None


def _case_indices(args) -> list[int]:
    if args.all_cases:
        if args.dataset_path is None or args.idx is None:
            raise SystemExit("--all_cases 需要 --dataset_path 与 --idx")
        ds = _load_dataset(args.dataset, args.dataset_path)
        n = len(ds.get_io_inputs(args.idx, max_count=0))
        if n == 0:
            raise SystemExit(f"idx={args.idx} 无测例")
        return list(range(n))
    return [args.input_idx]


def _load_question(args) -> str:
    if args.question:
        return args.question.strip()
    if args.question_file:
        return Path(args.question_file).expanduser().read_text(encoding="utf-8").strip()
    if args.dataset_path is not None and args.idx is not None:
        ds = _load_dataset(args.dataset, args.dataset_path)
        return ds.get_by_tag("description", args.idx)
    raise SystemExit("请指定 --question / --question_file 或 --dataset_path + --idx")


def _run_one_case(
    code: str,
    input_str: str,
    expected: str | None,
    *,
    timeout: int,
    case_idx: int,
) -> bool:
    print(f"\n=== case {case_idx} ===")
    print(f"[run_solve] input_len={len(input_str)} timeout={timeout}s")
    print("--- input (repr) ---")
    print(repr(input_str[:500] + ("..." if len(input_str) > 500 else "")))

    stdout, stderr = utils.run_solve_plain(
        code,
        input_str,
        timeout=timeout,
    )
    actual = _normalize_output(stdout)

    print("--- actual stdout ---")
    print(stdout)
    print("--- stderr ---")
    print(stderr or "(empty)")

    if expected is None:
        print("[compare] 无官方 output，跳过对比")
        return True

    exp_norm = _normalize_output(expected)
    ok = _outputs_match(actual, expected)
    print("--- expected stdout ---")
    print(expected)
    print(f"[compare] {'PASS' if ok else 'FAIL'}")
    if not ok:
        print("--- diff hint ---")
        print(f"actual lines={len(actual.splitlines())} expected lines={len(exp_norm.splitlines())}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="solver 生成 + run_solve + 与官方 output 对比"
    )
    parser.add_argument("--model_path", help="HF 模型路径（--code_file 时可省略）")
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--max_new_tokens", type=int, default=2048)
    parser.add_argument("--temperature", type=float, default=0.7)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--dataset",
        type=str,
        default="apps",
        choices=_DATASET_CHOICES,
        help="数据集类型（默认 apps）",
    )
    parser.add_argument("--dataset_path", help="数据集路径（例如 ~/datasets/codecontests）")
    parser.add_argument("--idx", type=int, default=0, help="题目 idx")
    parser.add_argument("--input_idx", type=int, default=0, help="第几条测例")
    parser.add_argument(
        "--all_cases",
        action="store_true",
        help="跑该题 input_output 中全部测例并分别对比",
    )
    parser.add_argument("--input", help='测例字符串，或 @path/to/input.txt')
    parser.add_argument("--expected", help='期望输出，或 @path/to/output.txt')
    parser.add_argument("--expected_file", help="期望输出文件路径")
    parser.add_argument("--question", help="题面（覆盖 dataset）")
    parser.add_argument("--question_file")
    parser.add_argument("--code_file", help="已有 Python 代码，跳过模型生成")
    parser.add_argument("--save_code", help="将生成代码写入该路径")
    parser.add_argument(
        "--prompt_file",
        default=str(ROOT / "prompt" / "solver.txt"),
        help="system prompt 路径",
    )
    parser.add_argument(
        "--gen_testcase",
        action="store_true",
        help="使用 prompt/testcaseganerator.txt 为指定题面生成一份小规模随机输入生成器代码（仅打印，不执行）",
    )
    parser.add_argument(
        "--testcase_prompt_file",
        default=str(ROOT / "prompt" / "testcaseganerator.txt"),
        help="testcase generator 的 system prompt 路径",
    )
    parser.add_argument(
        "--analyze_agreement",
        action="store_true",
        help="分析 Solver-Naive Agreement 与 Hidden Pass 的相关性",
    )
    parser.add_argument(
        "--agreement_jsonl",
        type=str,
        default="",
        help="Agreement 分析输入 jsonl（配合 --analyze_agreement）",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="outputs/agreement_analysis",
        help="Agreement 分析输出目录（报告 + 图）",
    )
    parser.add_argument(
        "--collect_agreement_probe",
        action="store_true",
        help="采集 Agreement probe 数据到 jsonl（默认 CURE_codecontests train）",
    )
    parser.add_argument(
        "--dataset_split",
        type=str,
        default="train",
        choices=["train", "test"],
        help="cure_codecontests 数据 split（collect 模式默认 train）",
    )
    parser.add_argument(
        "--num_solver",
        type=int,
        default=16,
        help="每题 Solver 样本数",
    )
    parser.add_argument(
        "--num_naive",
        type=int,
        default=16,
        help="每题 Naive 样本数",
    )
    parser.add_argument(
        "--probe_count",
        type=int,
        default=10,
        help="Agreement 输入条数 m（probe 模式生成数；hidden 模式取前 m 条 hidden input）",
    )
    parser.add_argument(
        "--agreement_source",
        type=str,
        default="hidden",
        choices=["probe", "hidden"],
        help="Agreement 输入来源：probe=生成器小输入；hidden=切 hidden 前 m 条",
    )
    parser.add_argument(
        "--naive_prompt_file",
        type=str,
        default=str(ROOT / "prompt" / "naivesolver.txt"),
        help="naive solver system prompt",
    )
    parser.add_argument(
        "--solver_lora",
        type=str,
        default="",
        help="Solver LoRA 目录（可选）",
    )
    parser.add_argument(
        "--naive_lora",
        type=str,
        default="",
        help="Naive LoRA 目录（可选）",
    )
    parser.add_argument("--gpu", type=str, default="", help="物理 GPU，如 1,2")
    parser.add_argument("--tensor_parallel_size", type=int, default=1)
    parser.add_argument("--gpu_memory_utilization", type=float, default=0.9)
    parser.add_argument("--max_lora_rank", type=int, default=64)
    parser.add_argument("--retry", type=int, default=2)
    parser.add_argument("--retry_sleep", type=float, default=5.0)
    parser.add_argument(
        "--exec_workers",
        type=int,
        default=32,
        help="probe/hidden 代码执行并行进程数（16 solver + 16 naive 时默认 32）",
    )
    parser.add_argument("--max_items", type=int, default=0, help="最多处理题数，0=不限")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--resume", action="store_true", help="跳过 jsonl 中已完成的题")
    args = parser.parse_args()

    if args.collect_agreement_probe:
        if not args.model_path:
            parser.error("--collect_agreement_probe 需要 --model_path")
        if not args.agreement_jsonl:
            args.agreement_jsonl = "outputs/agreement_probe.jsonl"
        if args.dataset == "apps" and not (args.dataset_path or "").strip():
            args.dataset = "cure_codecontests"
        _apply_gpu_env(args.gpu)
        collect_agreement_probe(args)
        return

    if args.analyze_agreement:
        if not args.agreement_jsonl:
            parser.error("--analyze_agreement 需要 --agreement_jsonl")
        run_agreement_analysis(args.agreement_jsonl, args.output_dir)
        return

    if args.gen_testcase:
        if not args.model_path:
            parser.error("--gen_testcase 需要 --model_path")
        model_dir = _validate_model_path(args.model_path)
        question = _load_question(args)
        system_prompt = utils.file2text(args.testcase_prompt_file)
        print(
            f"[gen_testcase] dataset={args.dataset} idx={args.idx} model={model_dir} device={args.device}"
        )
        Agent = _get_agent()
        agent = Agent(
            model_path=str(model_dir),
            system_prompt=system_prompt,
            device=args.device,
            trainable=False,
            use_lora=False,
        )
        code = agent.chat(
            question,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
        )
        code = utils.clean_code(code)
        print("--- generated testcase generator code ---")
        print(code)
        print("--- end code ---")
        return

    system_prompt = utils.file2text(args.prompt_file)

    if args.code_file:
        code = Path(args.code_file).expanduser().read_text(encoding="utf-8")
        code = utils.clean_code(code)
        print(f"[skip generate] 使用代码: {args.code_file}")
    else:
        if not args.model_path:
            parser.error("未指定 --code_file 时必须提供 --model_path")
        model_dir = _validate_model_path(args.model_path)
        question = _load_question(args)
        print(f"[generate] model={model_dir} device={args.device}")
        Agent = _get_agent()
        agent = Agent(
            model_path=str(model_dir),
            system_prompt=system_prompt,
            device=args.device,
            trainable=False,
            use_lora=False,
        )
        code = agent.chat(
            question,
            max_new_tokens=args.max_new_tokens,
            temperature=args.temperature,
        )
        code = utils.clean_code(code)
        print("--- generated code ---")
        print(code)
        print("--- end code ---")

    if args.save_code:
        out = Path(args.save_code).expanduser()
        out.write_text(code, encoding="utf-8")
        print(f"[saved] {out}")

    cases = _case_indices(args)
    passed = 0
    compared = 0
    for ci in cases:
        input_str, expected = _load_io_pair(args, ci)
        ok = _run_one_case(
            code,
            input_str,
            expected,
            timeout=args.timeout,
            case_idx=ci,
        )
        if expected is not None:
            compared += 1
            if ok:
                passed += 1

    if compared:
        print(f"\n[summary] {passed}/{compared} passed")
        if passed < compared:
            sys.exit(1)


if __name__ == "__main__":
    main()
