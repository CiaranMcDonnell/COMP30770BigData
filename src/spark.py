import findspark

findspark.init()
from numpy import broadcast
from pyspark import SparkConf, SparkContext
import time
from src.const import APP_NAME, FOREIGN_CCY, DOMESTIC_CCY
import psutil
import os
from pathlib import Path


def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


class SparkJob:
    def __init__(self, ts_file: str, fx_file: str):
        start_memory = get_memory_usage() / (1024**2)  # MB

        conf = (
            SparkConf()
            .setMaster("local")
            .setAppName(APP_NAME)
            .set("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=1024m")
            .set("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=1024m")
        )
        sc = SparkContext(conf=conf)
        sc.setLogLevel("OFF")

        start_time = time.time()

        csv_folder = Path(fx_file)

        ts_rdd = (
            sc.textFile(ts_file).map(self._parse_ts).filter(lambda x: x is not None)
        )
        fx_map = [
            (dom, foreign, sc.textFile(str(csv_folder / f"{dom}-{foreign}.csv")), [])
            for dom in DOMESTIC_CCY
            for foreign in FOREIGN_CCY
            if (csv_folder / f"{dom}-{foreign}.csv").exists()
        ]

        for domestic, foreign, rdd, res in fx_map:
            print(f"{domestic=} {foreign=}")
            # rdd.collect()
            res = rdd.reduce(
                lambda a, b: (
                    a[0] + b[0],
                    a[1] + b[1], 
                    a[2] + b[2],
                    a[3] + b[3], 
                    a[4] + b[4],
                )  
            )

        self._ts = ts_rdd
        self._fx = fx_map

        self.elapsed_time = (time.time() - start_time) * 1_000  # ms
        self.memory_used = get_memory_usage() / (1024**2) - start_memory

        self.sc = sc

    def kill(self):
        self.sc.stop()

    def ts(self):
        return self._ts

    def fx(self):
        return self._fx

    def memory(self):
        return self.memory_used

    def elapsed(self) -> float:
        return self.elapsed_time

    def dates(self):
        return list(self.data_dict.keys())

    def get_row(self, date_str: str):
        return self.data_dict.get(date_str, [])[0]

    def _parse_fx(self, line):
        fields = line.split(",")

        if fields[0].strip() == "Date":
            return None

        date = fields[0].strip()
        fx_rate = float(fields[1].strip())

        return (date, fx_rate)

    def _parse_ts(self, line):
        fields = line.split(",")

        if fields[0].strip() == "Date":
            return None

        date_val = fields[0].strip()
        euribor_rates = [float(rate.strip()) for rate in fields[1:6]]

        return (date_val, euribor_rates)
