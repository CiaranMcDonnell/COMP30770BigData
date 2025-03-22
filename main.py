from src.spark import SparkJob
from src.control import ControlJob
import argparse
from src.const import APP_NAME
import os
import matplotlib.pyplot as plt

os.environ["PYTHONHASHSEED"] = "0"  # otherwise spark complains about random hashing
os.environ["SPARK_LOCAL_IP"] = "172.18.20.168" # silence error


def parse_args():
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog=APP_NAME,
    )
    parser.add_argument("-ts_file", default="./data/ts/euribor.csv")
    parser.add_argument("-runs", default=50)
    parser.add_argument("-files", default=-1)
    return parser.parse_args()


def main():
    args = parse_args()

    TS_FILE = args.ts_file
    FX_FILE = f"./data/fx2"

    def validate_datasets():
        j0 = ControlJob(TS_FILE, FX_FILE)
        j1 = SparkJob(TS_FILE, FX_FILE)

        assert len(j0.fx()) == len(j1.fx())

       # for v0, v1 in zip(j0.fx(), j1.fx()):
       #     print(f"{v0[-1]}\n{v1[-1]}")
       # print(f"{j0.elapsed()=}, {j1.elapsed()=}")
        j1.kill()

    validate_datasets()

    def benchmark(runs):
        control_times = []
        spark_times = []

        control_memory = []
        spark_memory = []

        for _ in range(int(runs)):
            job = ControlJob(TS_FILE, FX_FILE)
            
            control_times.append(job.elapsed())
            control_memory.append(job.memory())
            job.kill()

        for _ in range(int(runs)):
            job = SparkJob(TS_FILE, FX_FILE)
            
            spark_times.append(job.elapsed())
            spark_memory.append(job.memory())
            job.kill()

        plt.plot(control_times, color="black")
        plt.plot(spark_times, color="red")
        plt.plot(control_memory, color="grey")
        plt.plot(spark_memory, color="orange")
        plt.legend(
            [
                "Control Load Time",
                "Spark Load Time",
                "Control Memory",
                "Spark Memory",
            ]
        )
        plt.title("Pandas Vs. Spark Financial Data Load Time")
        plt.show()

    benchmark(args.runs)


if __name__ == "__main__":
    main()
