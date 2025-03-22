import pandas as pd
import time
import matplotlib.pyplot as plt
import psutil
import os
from pathlib import Path
from collections import defaultdict
from src.const import FOREIGN_CCY, DOMESTIC_CCY


def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


class ControlJob:
    def __init__(self, ts_file: str, fx_file: str):
        start_memory = get_memory_usage() / (1024**2)  # MB
        self.time = time.time()

        csv_folder = Path(fx_file)

        df_map = [
            (dom, foreign, pd.read_csv(f"{dom}-{foreign}.csv"), [])
            for dom in DOMESTIC_CCY
            for foreign in FOREIGN_CCY
            if (csv_folder / f"{dom}-{foreign}.csv").exists()
        ]

        for dom, foreign, df, res in df_map:

            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

  
            sum_open = df["open"].sum()
            sum_low = df["low"].sum()
            sum_close = df["close"].sum()
            total_volume = df["volume"].sum()
            count = len(df)

            avg_open = sum_open / count if count else None
            avg_low = sum_low / count if count else None
            avg_close = sum_close / count if count else None

            res.append([dom, foreign, avg_open, avg_low, avg_close, total_volume])

        self._ts = pd.read_csv(ts_file)
        self._fx = df_map

        self.time = (time.time() - self.time) * 1_000  # ms
        self.memory_used = get_memory_usage() / (1024**2) - start_memory

    def memory(self):
        return self.memory_used

    def elapsed(self) -> float:
        return self.time

    def ts(self):
        return self._ts

    def fx(self):
        return self._fx
