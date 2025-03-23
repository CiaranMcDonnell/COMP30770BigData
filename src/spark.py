import findspark

findspark.init()
import logging

logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
from pyspark import SparkContext
import time
from src.const import APP_NAME
import psutil
import os
import pandas as pd
import glob
import subprocess
import gc
from src.spark_conf import get_spark_conf
from typing import Any

JVM = Any
Runtime = Any


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


def get_jvm_memory_usage():
    try:
        jps_output = subprocess.run(
            ["jps"], capture_output=True, text=True, check=False
        )

        if jps_output.returncode == 0:
            spark_pids = []
            for line in jps_output.stdout.splitlines():
                if "SparkSubmit" in line or "Spark" in line:
                    spark_pids.append(line.split()[0])

            total_memory = 0
            for pid in spark_pids:
                try:
                    result = subprocess.run(
                        ["ps", "-o", "rss=", "-p", pid],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    if result.stdout.strip():
                        total_memory += int(result.stdout.strip()) * 1024
                except Exception:
                    continue

            return total_memory
    except Exception:
        return 0


class SparkJob:
    def __init__(self, fx_file=None):
        gc.collect()
        mem_python = get_memory_usage()
        mem_jvm = get_jvm_memory_usage()
        start_memory_python = mem_python / (1024**2) if mem_python else 0
        start_memory_jvm = mem_jvm / (1024**2) if mem_jvm else 0
        start_memory_total = start_memory_python + start_memory_jvm
        
        self.memory_checkpoints = []
        self.total_memory_allocated = 0
        
        self.memory_checkpoints.append({
            'stage': 'init',
            'python_memory': start_memory_python,
            'jvm_memory': start_memory_jvm,
            'total_memory': start_memory_total,
            'delta': 0,
            'cumulative': 0
        })

        if fx_file is None:
            raise ValueError("FX file path must be provided")

        conf = get_spark_conf(APP_NAME)
        sc = SparkContext(conf=conf)
        sc.setLogLevel("OFF")
        
        gc.collect()
        mem_python_sc = get_memory_usage() / (1024**2) if get_memory_usage() else 0
        mem_jvm_sc = get_jvm_memory_usage() / (1024**2) if get_jvm_memory_usage() else 0
        memory_sc_total = mem_python_sc + mem_jvm_sc
        memory_delta_sc = max(0, memory_sc_total - start_memory_total)
        self.total_memory_allocated += memory_delta_sc
        
        self.memory_checkpoints.append({
            'stage': 'spark_init',
            'python_memory': mem_python_sc,
            'jvm_memory': mem_jvm_sc,
            'total_memory': memory_sc_total,
            'delta': memory_delta_sc,
            'cumulative': self.total_memory_allocated
        })

        start_time = time.time()

        parquet_files = glob.glob(os.path.join(fx_file, "*.parquet"))

        file_count = len(parquet_files)
        optimal_partitions = min(max(4, file_count // 4), 16)
        file_rdd = sc.parallelize(parquet_files, numSlices=optimal_partitions)
        
        gc.collect()
        mem_python_rdd = get_memory_usage() / (1024**2) if get_memory_usage() else 0
        mem_jvm_rdd = get_jvm_memory_usage() / (1024**2) if get_jvm_memory_usage() else 0
        memory_rdd_total = mem_python_rdd + mem_jvm_rdd
        memory_delta_rdd = max(0, memory_rdd_total - memory_sc_total)
        self.total_memory_allocated += memory_delta_rdd
        
        self.memory_checkpoints.append({
            'stage': 'rdd_creation',
            'python_memory': mem_python_rdd,
            'jvm_memory': mem_jvm_rdd,
            'total_memory': memory_rdd_total,
            'delta': memory_delta_rdd,
            'cumulative': self.total_memory_allocated
        })

        def get_stats_from_parquet(file_path):
            try:
                basename = os.path.basename(file_path)
                pair = os.path.splitext(basename)[0].split("-")
                if len(pair) != 2:
                    return None

                dom, foreign = pair

                df = pd.read_parquet(
                    file_path, columns=["open", "low", "close", "volume"]
                )

                avg_open = df["open"].mean()
                avg_low = df["low"].mean()
                avg_close = df["close"].mean()
                total_volume = df["volume"].sum()

                return ((dom, foreign), ([avg_open, avg_low, avg_close, total_volume], 1))
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                return None

        valid_results = file_rdd.map(get_stats_from_parquet).filter(lambda x: x is not None)
        
        gc.collect()
        mem_python_map = get_memory_usage() / (1024**2) if get_memory_usage() else 0
        mem_jvm_map = get_jvm_memory_usage() / (1024**2) if get_jvm_memory_usage() else 0
        memory_map_total = mem_python_map + mem_jvm_map
        memory_delta_map = max(0, memory_map_total - memory_rdd_total)
        self.total_memory_allocated += memory_delta_map
        
        self.memory_checkpoints.append({
            'stage': 'map_operation',
            'python_memory': mem_python_map,
            'jvm_memory': mem_jvm_map,
            'total_memory': memory_map_total,
            'delta': memory_delta_map,
            'cumulative': self.total_memory_allocated
        })

        def reduce_stats(stats_count1, stats_count2):
            stats1, count1 = stats_count1
            stats2, count2 = stats_count2
            
            combined_stats = [
                (stats1[0] * count1 + stats2[0] * count2) / (count1 + count2),  # avg_open
                (stats1[1] * count1 + stats2[1] * count2) / (count1 + count2),  # avg_low
                (stats1[2] * count1 + stats2[2] * count2) / (count1 + count2),  # avg_close
                stats1[3] + stats2[3]  # total_volume (sum)
            ]
            
            return (combined_stats, count1 + count2)

        reduced_results = valid_results.reduceByKey(reduce_stats)
        
        gc.collect()
        mem_python_reduce = get_memory_usage() / (1024**2) if get_memory_usage() else 0
        mem_jvm_reduce = get_jvm_memory_usage() / (1024**2) if get_jvm_memory_usage() else 0
        memory_reduce_total = mem_python_reduce + mem_jvm_reduce
        memory_delta_reduce = max(0, memory_reduce_total - memory_map_total)
        self.total_memory_allocated += memory_delta_reduce
        
        self.memory_checkpoints.append({
            'stage': 'reduce_operation',
            'python_memory': mem_python_reduce,
            'jvm_memory': mem_jvm_reduce,
            'total_memory': memory_reduce_total,
            'delta': memory_delta_reduce,
            'cumulative': self.total_memory_allocated
        })

        fx_results = reduced_results.collect()
        fx_map = []
        for (dom, foreign), (stats, count) in fx_results:
            dummy_rdd = sc.parallelize([1])
            fx_map.append([dom, foreign, dummy_rdd, stats])

        self._fx = fx_map
        
        gc.collect()
        mem_python_collect = get_memory_usage() / (1024**2) if get_memory_usage() else 0
        mem_jvm_collect = get_jvm_memory_usage() / (1024**2) if get_jvm_memory_usage() else 0
        memory_collect_total = mem_python_collect + mem_jvm_collect
        memory_delta_collect = max(0, memory_collect_total - memory_reduce_total)
        self.total_memory_allocated += memory_delta_collect
        
        self.memory_checkpoints.append({
            'stage': 'collect_results',
            'python_memory': mem_python_collect,
            'jvm_memory': mem_jvm_collect,
            'total_memory': memory_collect_total,
            'delta': memory_delta_collect,
            'cumulative': self.total_memory_allocated
        })

        self.elapsed_time = (time.time() - start_time) * 1_000

        try:
            jvm: JVM = sc._jvm
            runtime: Runtime = jvm.java.lang.Runtime.getRuntime()
            total_mem = runtime.totalMemory() / (1024 * 1024)
            free_mem = runtime.freeMemory() / (1024 * 1024)
            used_mem = total_mem - free_mem
            self.peak_memory = used_mem
        except Exception:
            self.peak_memory = memory_collect_total - start_memory_total

        self.memory_used = self.total_memory_allocated
        self.sc = sc

    def kill(self):
        self.sc.stop()

    def fx(self):
        return self._fx

    def memory(self):
        return self.memory_used
        
    def peak_memory_usage(self):
        return self.peak_memory
        
    def memory_timeline(self):
        if not self.memory_checkpoints:
            return {
                'timeline': [],
                'peak_rss': 0,
                'final_rss': 0,
                'total_files_processed': 0
            }
            
        first_checkpoint = self.memory_checkpoints[0]
        memory_key = 'after' if 'after' in first_checkpoint else 'total_memory'
        
        peak_rss = max(checkpoint[memory_key] for checkpoint in self.memory_checkpoints)
        
        final_rss = self.memory_checkpoints[-1][memory_key]
        
        for i in range(1, len(self.memory_checkpoints)):
            prev_after = self.memory_checkpoints[i-1][memory_key]
            self.memory_checkpoints[i]['delta_from_prev'] = self.memory_checkpoints[i][memory_key] - prev_after

        if 'delta' in first_checkpoint:
            self.memory_checkpoints[0]['delta_from_prev'] = self.memory_checkpoints[0]['delta']
        else:
            self.memory_checkpoints[0]['delta_from_prev'] = self.memory_checkpoints[0][memory_key]
        
        memory_metrics = {
            'timeline': self.memory_checkpoints,
            'peak_rss': peak_rss,
            'final_rss': final_rss,
            'total_files_processed': len(self.memory_checkpoints)
        }
        
        return memory_metrics

    def elapsed(self):
        return self.elapsed_time
    
    def get_results(self):
        results = []
        for dom, foreign, _, stats in self._fx:
            results.append({
                "dom_currency": dom,
                "foreign_currency": foreign,
                "avg_open": stats[0],
                "avg_low": stats[1],
                "avg_close": stats[2],
                "total_volume": stats[3]
            })
        return pd.DataFrame(results)
        
    def save_results(self, output_path):
        results_df = self.get_results()
        results_df.to_parquet(output_path, index=False)