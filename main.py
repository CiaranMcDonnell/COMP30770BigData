from src.spark import SparkJob
from src.control import ControlJob
import argparse
from src.const import APP_NAME
from src.finance import Model
import os
import matplotlib.pyplot as plt

os.environ["PYTHONHASHSEED"] = "0"  # otherwise spark complains about random hashing


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

    if int(args.files) != -1:
        from src.const import FOREIGN_CCY, DOMESTIC_CCY
        DOMESTIC_CCY = DOMESTIC_CCY[:int(args.files)]
        FOREIGN_CCY = FOREIGN_CCY[:int(args.files)]


    def validate_datasets():
        j0 = ControlJob(TS_FILE, FX_FILE)
        j1 = SparkJob(TS_FILE, FX_FILE)
        

        assert len(j0.fx()) == len(j1.fx())

        for v0, v1 in zip(j0.fx(), j1.fx()):
            print(f"{v0[-1]}\n{v1[-1]}")

        j1.kill()


    validate_datasets()

    # list of dates we calibrate with
    # can be any dates present in both csvs
    dates: list[str] = ["2020-03-03", "2005-06-08", "2009-12-21", "2010-09-01"]

    def benchmark(runs):
        control_job = ControlJob(TS_FILE, FX_FILE)
        spark_job = SparkJob(TS_FILE, FX_FILE)

        control_times = [control_job.elapsed()]
        spark_times = [spark_job.elapsed()]

        control_memory = []
        spark_memory = []

        for _ in range(int(runs)):
            control_model = Model(control_job, dates)

            control_times.append(control_model.elapsed())
            control_memory.append(control_model.memory())

        for _ in range(int(runs)):
            spark_model = Model(spark_job, dates)

            spark_times.append(spark_model.elapsed())
            spark_memory.append(spark_model.memory())

        plt.plot(control_times, color="black")
        plt.plot(spark_times, color="red")
        # plt.plot(control_memory, color="grey")
        # plt.plot(spark_memory, color="orange")
        plt.legend(
            [
                "Control Calibration Time",
                "Spark Calibration Time",
                "Control Memory",
                "Spark Memory",
            ]
        )
        plt.title("Benchmarking of FX Calibration* Using Spark and Pandas")
        plt.show()

    benchmark(args.runs)


if __name__ == "__main__":
    main()
