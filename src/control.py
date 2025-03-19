import pandas as pd
import time
import matplotlib.pyplot as plt
import psutil
import os

def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss 

class ControlJob:
    def __init__(self, ts_file: str, fx_file: str):
        start_memory = get_memory_usage() / (1024 ** 2) #MB
        self.time = time.time()

        fx_data = pd.read_csv(fx_file)
        ts_data = pd.read_csv(ts_file)
        
        joined_data = pd.merge(fx_data, ts_data, on='Date', how='inner')
        joined_data['TS'] = joined_data[['1w', '1m', '3m', '6m', '12m']].apply(lambda row: row.tolist(), axis=1)
        joined_data['FX'] = joined_data[f"{fx_file.split('/')[-1].split('.')[0]}_Open"]
        self.data = joined_data[['Date', 'FX', 'TS']]

        self.time = time.time() - self.time
        self.memory_used = get_memory_usage() / (1024 ** 2) - start_memory

    def memory(self):
        return self.memory_used

    def elapsed(self) -> float:
        return self.time

    def _(self):
        return self.data

    def get_row(self, date: str):
        return self.data.loc[self.data['Date'] == date]
