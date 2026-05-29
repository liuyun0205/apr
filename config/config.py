import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--basemodel", type=str, default="None")
    parser.add_argument("--model_type", type=str, default="own", choices=["own", "local", "api"])
    parser.add_argument(
        "--base_url",
        type=str,
        default="https://api.chshapi.org",
        help="直连 API 地址（newapi_channel_conn.url）",
    )
    parser.add_argument("--api_key", type=str, default="", help="直连 API key（也可用环境变量）")

    # DeepSpeed ZeRO（仅在 model_type=local 且安装 deepspeed 时生效）
    parser.add_argument("--use_zero", action="store_true", help="Enable DeepSpeed ZeRO for local model")
    parser.add_argument("--zero_stage", type=int, default=3, choices=[1, 2, 3], help="ZeRO optimization stage")
    parser.add_argument(
        "--zero_offload",
        type=str,
        default="none",
        choices=["none", "cpu"],
        help="Offload optimizer/params to CPU when using ZeRO",
    )
    return parser.parse_args()

