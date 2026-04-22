import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--basemodel", type=str, default="None")
    parser.add_argument("--model_type", type=str, default="own")

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