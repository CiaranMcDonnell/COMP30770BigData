from scipy.optimize import minimize
import numpy as np
import time
from src.control import ControlJob
from src.spark import SparkJob
import os
import psutil


def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


class Model:
    def __init__(self, job: SparkJob | ControlJob, dates: list[str]):
        start_memory = get_memory_usage() / (1024**2)  # MB

        self.dates = dates
        self.job = job

        self.time = time.time()

        for date in dates:
            row = job.get_row(date)
            if isinstance(job, ControlJob):
                self.date = date
                self.fx = row["FX"].iloc[0]
                self.ts = row["TS"].iloc[0]
            else:
                self.date = date
                self.fx = row[1]
                self.ts = row[2]
            self.calibrate()

        self.time = time.time() - self.time
        self.memory_used = get_memory_usage() / (1024**2) - start_memory

    def elapsed(self):
        return self.time

    def calibrate(self):
        res = minimize(self._obj, x0=[0.5], method="Nelder-Mead")
        self._sigma = res.x[0]

    def _obj(self, x):
        eps: float = 0.0

        for val in self.ts:
            eps += (self.fx - x * val) ** 2
        return eps

    def sigma(self):
        return self._sigma

    def memory(self):
        return self.memory_used
