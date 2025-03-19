import findspark

findspark.init()
from numpy import broadcast
from pyspark import SparkConf, SparkContext
import time
from .const import APP_NAME
import psutil
import os


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

        fx_data = (
            sc.textFile(fx_file).map(self._parse_fx).filter(lambda x: x is not None)
        )
        ts_data = (
            sc.textFile(ts_file).map(self._parse_ts).filter(lambda x: x is not None)
        )

        ts_dict = dict(ts_data.collect())
        broadcast_ts = sc.broadcast(ts_dict)

        # Define a function to join each FX record with the broadcasted Euribor data
        def join_with_euribor(record):
            # record is (date, fx_rate)
            date, fx_rate = record
            euribor_rates = broadcast_ts.value.get(date)
            if euribor_rates is not None:
                return (date, fx_rate, euribor_rates)
            else:
                return None

        joined_data = fx_data.map(join_with_euribor).filter(lambda x: x is not None)

        # instead of collecting to a list, group by date for fast lookup
        # map to pairs where the key is the date, then group all records by date
        data_by_date = (
            joined_data.map(lambda record: (record[0], record))
            .groupByKey()
            .mapValues(list)
            .collectAsMap()
        )

        self.data_dict = data_by_date
        self.elapsed_time = (time.time() - start_time) * 1_000 # ms
        self.memory_used = get_memory_usage() / (1024**2) - start_memory

        self.sc = sc

    def kill(self):
        self.sc.stop()

    def get(self):
        return self.data_dict

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
