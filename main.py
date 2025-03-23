from src.spark import SparkJob
from src.control import ControlJob
import argparse
from src.const import APP_NAME
import os
import time
import numpy as np
import glob
import json

os.environ["PYTHONHASHSEED"] = "0"  # otherwise spark complains about random hashing
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


def parse_args():
    parser = argparse.ArgumentParser(prog=APP_NAME)
    parser.add_argument(
        "-fx_path", default="./data/fx", help="Path to FX Parquet files directory"
    )
    parser.add_argument("-runs", default=1, help="Number of benchmark runs")
    parser.add_argument(
        "-files", default=-1, help="Number of files to process (-1 for all)"
    )
    parser.add_argument(
        "-output_dir", default="./data/output", help="Directory to save output results"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    FX_FILE = args.fx_path
    OUTPUT_DIR = args.output_dir
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    def validate_datasets():
        print("Creating SparkJob...")
        start_time = time.time()
        j1 = SparkJob(fx_file=FX_FILE)
        spark_time = time.time() - start_time

        print("Creating ControlJob...")
        start_time = time.time()
        j0 = ControlJob(fx_file=FX_FILE)
        control_time = time.time() - start_time

        print(f"SparkJob FX count: {len(j1.fx())}")
        print(f"ControlJob FX count: {len(j0.fx())}")

        if len(j0.fx()) != len(j1.fx()):
            print(f"WARNING: FX count mismatch: {len(j0.fx())} vs {len(j1.fx())}")

        print(f"SparkJob processing time: {spark_time:.2f} seconds")
        print(f"ControlJob processing time: {control_time:.2f} seconds")
        print(f"Speedup: {control_time/spark_time:.2f}x")
        
        spark_results = j1.get_results()
        control_results = j0.get_results()
        
        j1.save_results(os.path.join(OUTPUT_DIR, "spark_validation_results.parquet"))
        j0.save_results(os.path.join(OUTPUT_DIR, "control_validation_results.parquet"))
        
        with open(os.path.join(OUTPUT_DIR, "spark_memory_timeline.json"), "w") as f:
            json.dump(j1.memory_timeline(), f, indent=2)
            
        with open(os.path.join(OUTPUT_DIR, "control_memory_timeline.json"), "w") as f:
            json.dump(j0.memory_timeline(), f, indent=2)
        
        with open(os.path.join(OUTPUT_DIR, "control_memory_metrics.json"), "w") as f:
            memory_data = j0.memory_timeline()
            summary_metrics = {k: v for k, v in memory_data.items() if k != 'timeline'}
            json.dump(summary_metrics, f, indent=2)
            
        with open(os.path.join(OUTPUT_DIR, "spark_memory_metrics.json"), "w") as f:
            memory_data = j1.memory_timeline()
            summary_metrics = {k: v for k, v in memory_data.items() if k != 'timeline'}
            json.dump(summary_metrics, f, indent=2)
        
        print(f"Validation results saved to {OUTPUT_DIR}")

        j1.kill()

    validate_datasets()

    def benchmark(runs):
        print(f"Running benchmark with {runs} iterations...")

        control_times = []
        spark_times = []
        control_memory = []
        control_peak_memory = []
        spark_memory = []
        spark_peak_memory = []
        file_counts = []

        all_files = glob.glob(os.path.join(FX_FILE, "*.parquet"))
        if args.files == -1:
            files_to_process = all_files
        else:
            files_to_process = all_files[:int(args.files)]
            print(f"Limiting processing to {args.files} files out of {len(all_files)} total")
        total_files = len(files_to_process)
        print(f"Total Parquet files to process: {total_files}")

        limited_fx_dir = os.path.join(os.path.dirname(FX_FILE), "limited_fx")
        os.makedirs(limited_fx_dir, exist_ok=True)

        for file in glob.glob(os.path.join(limited_fx_dir, "*.parquet")):
            os.remove(file)

        for file_path in files_to_process:
            target_path = os.path.join(limited_fx_dir, os.path.basename(file_path))
            os.symlink(os.path.abspath(file_path), target_path)

        print("Benchmarking Control implementation...")
        for i in range(int(runs)):
            print(f"  Run {i+1}/{runs}")
            job = ControlJob(fx_file=limited_fx_dir)
            control_times.append(job.elapsed())
            control_memory.append(job.memory())
            control_peak_memory.append(job.peak_memory_usage())
            file_counts.append(len(job.fx()))
            
            run_output_dir = os.path.join(OUTPUT_DIR, f"run_{i+1}")
            os.makedirs(run_output_dir, exist_ok=True)
            job.save_results(os.path.join(run_output_dir, "control_results.parquet"))
            
            with open(os.path.join(run_output_dir, "control_memory_timeline.json"), "w") as f:
                json.dump(job.memory_timeline(), f, indent=2)
                
            with open(os.path.join(run_output_dir, "control_memory_metrics.json"), "w") as f:
                memory_data = job.memory_timeline()

                summary_metrics = {k: v for k, v in memory_data.items() if k != 'timeline'}
                json.dump(summary_metrics, f, indent=2)
            
            job.kill()

        print("Benchmarking Spark implementation...")
        for i in range(int(runs)):
            print(f"  Run {i+1}/{runs}")
            job = SparkJob(fx_file=limited_fx_dir)
            spark_times.append(job.elapsed())
            spark_memory.append(job.memory())
            spark_peak_memory.append(job.peak_memory_usage())
            
            run_output_dir = os.path.join(OUTPUT_DIR, f"run_{i+1}")
            os.makedirs(run_output_dir, exist_ok=True)
            job.save_results(os.path.join(run_output_dir, "spark_results.parquet"))
            
            with open(os.path.join(run_output_dir, "spark_memory_timeline.json"), "w") as f:
                json.dump(job.memory_timeline(), f, indent=2)
                
            with open(os.path.join(run_output_dir, "spark_memory_metrics.json"), "w") as f:
                memory_data = job.memory_timeline()
                summary_metrics = {k: v for k, v in memory_data.items() if k != 'timeline'}
                json.dump(summary_metrics, f, indent=2)
            
            job.kill()

        avg_control_time = np.mean(control_times)
        avg_spark_time = np.mean(spark_times)
        speedup = avg_control_time / avg_spark_time if avg_spark_time > 0 else 0

        avg_control_memory = np.mean(control_memory)
        avg_spark_memory = np.mean(spark_memory)
        memory_ratio = avg_control_memory / avg_spark_memory if avg_spark_memory > 0 else 0
        
        avg_control_peak = np.mean(control_peak_memory)
        avg_spark_peak = np.mean(spark_peak_memory)
        peak_memory_ratio = avg_control_peak / avg_spark_peak if avg_spark_peak > 0 else 0

        print("\nBenchmark Results (FX Files Only):")
        print(f"Files processed: {file_counts[0]}")
        print(f"Average Control Time: {avg_control_time:.2f} ms")
        print(f"Average Spark Time: {avg_spark_time:.2f} ms")
        print(f"Speedup: {speedup:.2f}x")
        print(f"Total Control Memory: {avg_control_memory:.2f} MB")
        print(f"Total Spark Memory: {avg_spark_memory:.2f} MB")
        print(f"Memory Ratio: {memory_ratio:.2f}x")
        print(f"Peak Control Memory: {avg_control_peak:.2f} MB")
        print(f"Peak Spark Memory: {avg_spark_peak:.2f} MB")
        print(f"Peak Memory Ratio: {peak_memory_ratio:.2f}x")

        benchmark_summary = {
            "files_processed": file_counts[0],
            "avg_control_time_ms": avg_control_time,
            "avg_spark_time_ms": avg_spark_time,
            "speedup": speedup,
            "total_control_memory_mb": avg_control_memory,
            "total_spark_memory_mb": avg_spark_memory,
            "memory_ratio": memory_ratio,
            "peak_control_memory_mb": avg_control_peak,
            "peak_spark_memory_mb": avg_spark_peak,
            "peak_memory_ratio": peak_memory_ratio
        }
        
        with open(os.path.join(OUTPUT_DIR, "benchmark_summary.json"), "w") as f:
            json.dump(benchmark_summary, f, indent=2)
            
        print(f"All results saved to {OUTPUT_DIR}")

    benchmark(args.runs)


if __name__ == "__main__":
    main()
