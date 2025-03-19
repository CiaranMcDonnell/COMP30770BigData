from src.spark import SparkJob
from src.control import ControlJob
import logging
import argparse
from src.const import APP_NAME
from src.finance import Model
import os
import matplotlib.pyplot as plt

os.environ["PYTHONHASHSEED"] = "0"

TS_FILE: str = ""
FX_FILE: str = ""

logger = logging.getLogger(__name__)


def main():
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog=APP_NAME,
    )
    parser.add_argument("-ts_file", default="./data/euribor.csv")
    parser.add_argument("-dom-ccy", default="EUR")
    parser.add_argument("-for-ccy", default="JPY")
    parser.add_argument("-runs", default=50)
    args = parser.parse_args()

    TS_FILE = args.ts_file
    FX_FILE = f"./data/{args.dom_ccy}{args.for_ccy}.csv"

    def validate_datasets():
        j0 = ControlJob(TS_FILE, FX_FILE)
        j1 = SparkJob(TS_FILE, FX_FILE)

        assert len(j0._()) == len(j1._())

        j1.kill()

    validate_datasets()

    # list of dates we calibrate with
    dates: list[str] = ["2020-03-03", "2005-06-08", "2009-12-21", "2010-09-01"]

    def benchmark(runs):
        control_times = []
        spark_times = []

        control_memory = []
        spark_memory = []

        control_job = ControlJob(TS_FILE, FX_FILE)
        spark_job = SparkJob(TS_FILE, FX_FILE)

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
        plt.show()

    benchmark(args.runs)


if __name__ == "__main__":
    main()
