"""Console script for backtest."""

import fire


def help():
    print("backtest")
    print("=" * len("backtest"))
    print("backtest framework")


def main():
    fire.Fire({"help": help})


if __name__ == "__main__":
    main()  # pragma: no cover
