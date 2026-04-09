import argparse
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--basemodel", type=str, default="None")
    parser.add_argument('--model_type', type=str, default='own')
    return parser.parse_args()