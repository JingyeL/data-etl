import argparse
from datetime import datetime


def parse_args():
    args =  argparse.ArgumentParser(description="Test the lambda functions")
    args.add_argument(
        "--start",
        "-s",
        type=str,
        required=True
    )
    args.add_argument(
        "--end",
        "-e",
        type=str,
        required=True
    )
    return args.parse_args()

def main():

    args = parse_args()
    start = args.start
    end = args.end

    time_start = datetime.strptime(start, "%H:%M:%S.%f")
    time_end = datetime.strptime(end, "%H:%M:%S.%f")
    time_diff = time_end - time_start
    print(f"Time difference: {time_diff.seconds} seconds")


if __name__ == "__main__":
    main()