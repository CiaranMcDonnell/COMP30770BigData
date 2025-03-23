import pandas as pd
import time
import psutil
import os
import glob
import gc


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


class ControlJob:
    def __init__(self, fx_file=None):
        if fx_file is None:
            raise ValueError("FX file path must be provided")

        start_memory = get_memory_usage() / (1024**2)
        self.time = time.time()
        self.peak_memory = start_memory
        self.memory_checkpoints = []

        parquet_files = glob.glob(os.path.join(fx_file, "*.parquet"))

        df_map = []
        total_memory_allocated = 0
        
        for file_path in parquet_files:
            try:
                # Collect garbage before processing each file
                gc.collect()
                before_file_memory = get_memory_usage() / (1024**2)
                
                basename = os.path.basename(file_path)
                pair = os.path.splitext(basename)[0].split("-")

                if len(pair) != 2:
                    continue

                dom, foreign = pair

                df = pd.read_parquet(
                    file_path, columns=["open", "low", "close", "volume"]
                )

                avg_open = df["open"].mean()
                avg_low = df["low"].mean()
                avg_close = df["close"].mean()
                total_volume = df["volume"].sum()

                result = [avg_open, avg_low, avg_close, total_volume]

                sample_df = df.iloc[:100] if len(df) > 100 else df

                df_map.append((dom, foreign, sample_df, result))

                del df
                gc.collect()

                after_file_memory = get_memory_usage() / (1024**2)
                memory_delta = max(0, after_file_memory - before_file_memory)
                total_memory_allocated += memory_delta
                
                self.peak_memory = max(self.peak_memory, after_file_memory - start_memory)
                
                self.memory_checkpoints.append({
                    'file': basename,
                    'before': before_file_memory,
                    'after': after_file_memory,
                    'delta': memory_delta,
                    'cumulative': total_memory_allocated
                })

            except Exception as e:
                print(f"Error processing control {basename}: {str(e)}")

        self._fx = df_map

        self.time = (time.time() - self.time) * 1_000
        self.memory_used = total_memory_allocated

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
            
        peak_rss = max(checkpoint['after'] for checkpoint in self.memory_checkpoints)
        
        final_rss = self.memory_checkpoints[-1]['after'] if self.memory_checkpoints else 0
        
        for i in range(1, len(self.memory_checkpoints)):
            prev_after = self.memory_checkpoints[i-1]['after']
            self.memory_checkpoints[i]['delta_from_prev'] = self.memory_checkpoints[i]['after'] - prev_after
        
        if self.memory_checkpoints:
            self.memory_checkpoints[0]['delta_from_prev'] = self.memory_checkpoints[0]['delta']
        
        memory_metrics = {
            'timeline': self.memory_checkpoints,
            'peak_rss': peak_rss,
            'final_rss': final_rss,
            'total_files_processed': len(self.memory_checkpoints)
        }
        
        return memory_metrics

    def elapsed(self):
        return self.time

    def fx(self):
        return self._fx

    def kill(self):
        pass
        
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
