import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--basemodel", type=str, default="None")
    parser.add_argument("--model_type", type=str, default="own", choices=["own", "local", "api"])
    parser.add_argument("--path",type=str)
    parser.add_argument(
        "--base_url",
        type=str,
        default="https://api.chshapi.org",
        help="直连 API 地址（newapi_channel_conn.url）",
    )
    parser.add_argument("--api_key", type=str, default="", help="直连 API key（也可用环境变量）")

    # vLLM（model_type=local）；多卡用 --tensor-parallel-size 或 CUDA_VISIBLE_DEVICES
    parser.add_argument(
        "--tensor_parallel_size",
        type=int,
        default=1,
        help="vLLM tensor parallel size (local model only)",
    )
    parser.add_argument(
        "--gpu_memory_utilization",
        type=float,
        default=0.9,
        help="vLLM GPU memory utilization (local model only)",
    )
    # 已废弃：local 改用 vLLM
    parser.add_argument("--use_zero", action="store_true", help="(deprecated) ignored")
    return parser.parse_args()

