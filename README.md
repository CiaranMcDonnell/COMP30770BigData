# Big Data FX Calibration Project

This project completed for COMP30770 demonstrates how Apache Spark can optimize large-scale FX data processing. Below are the benchmark highlights comparing a traditional pandas-based approach with a Spark-based MapReduce solution.

## Project Overview

The implementation includes two approaches:
- **Traditional approach**: Sequential processing using pandas on a single machine
- **Distributed approach**: Parallel processing using Spark's MapReduce paradigm, leveraging the lower-level Spark Core RDD API (instead of DataFrame or SQL libraries)

Key metrics tracked include:
- Execution time
- Memory usage (peak and cumulative)
- Processing efficiency

## Dataset Source

The data was sourced from [ECB FX Rates](https://data.humdata.org/dataset/ecb-fx-rates), but the original link for downloading the full 31GB of historical FX quotations is now defunct. It might be necessary to locate an alternative mirror or use a locally stored copy to access the data.

## System Requirements

- Python 3.13+
- Java 11-17 (recommended: Amazon Corretto 17)
- Apache Spark 3.x
- Apache Hadoop
- macOS (for Homebrew-based installation)

## Installation

### Prerequisites

```bash
# Install SDKMAN for Java version management
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# Install Homebrew if not already installed (macOS)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install UV package manager
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Full Setup

```bash
# Install and use Java 17
sdk install java 17.0.14-amzn 
sdk use java 17.0.14-amzn 

# Install Hadoop and Spark
brew install hadoop
brew install apache-spark

# Create and activate virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv sync
```

## Usage

Run the main application with:

```bash
uv run main.py -fx_path ./data/fx -runs 1 -output_dir ./data/output
```

or more simply,

```bash
uv run main.py -runs 1
```

Parameters:
- `-fx_path`: Path to the directory containing FX Parquet files
- `-runs`: Number of benchmark runs
- `-files`: Number of files to process (-1 for all, or leave default)
- `-output_dir`: Directory to save output results

## Project Structure

- `src/control.py`: Implementation of the traditional sequential processing approach
- `src/spark.py`: Implementation of the distributed processing using Spark RDD API
- `main.py`: Main entry point that runs benchmarks and comparisons
- `data/`: Directory for input and output data

## Implementation Details

### Traditional Approach
The `ControlJob` class processes files sequentially using pandas, tracking memory usage at each step.

### Distributed Approach
The `SparkJob` class uses Spark Core RDD API to process files in parallel, following MapReduce principles:
- Map: Extract statistics from each Parquet file
- ReduceByKey: Combine results by currency pair

## Memory Profiling

The implementation tracks:
- Peak RSS (Resident Set Size): Maximum memory usage
- Final RSS: Memory still held at completion
- RSS per file: Memory used for each file processing step

## Analysis Results

Results are saved to the specified output directory, containing:
- JSON files with memory usage timelines
- Memory metrics summaries
- Performance benchmark results
- Parquet files with processed statistical results

## Performance Metrics

| Dataset (GB) | Files | Traditional Time (ms) | Spark Time (ms) | Speedup |
|--------------|-------|------------------------|-----------------|---------|
| 1            | 35    | 1558.07               | 1672.98         | 0.93x   |
| 5            | 160   | 7414.02               | 2876.37         | 2.58x   |
| 10           | 320   | 19907.69              | 4672.81         | 4.26x   |
| 31           | 1000  | 84493.32              | 11616.90        | 7.27x   |

## Memory Usage Metrics

| Dataset (GB) | Total Trad. Mem (MB) | Total Spark Mem (MB) | Mem Ratio | Peak Trad. Mem (MB) | Peak Spark Mem (MB) | Peak Mem Ratio |
|--------------|----------------------|-----------------------|-----------|----------------------|---------------------|-----------------|
| 1            | 848.24              | 25.17                | 33.71x    | 3291.73             | 52.44               | 62.77x          |
| 5            | 1743.89             | 84.12                | 20.73x    | 3553.19             | 152.42              | 23.31x          |
| 10           | 3461.80             | 44.89                | 77.13x    | 3511.08             | 176.82              | 19.86x          |
| 31           | 10893.47            | 9.12                 | 1193.80x  | 3035.32             | 86.04               | 35.28x          |

## Data Examples

Below is an example of raw JSON input (scaled down floating numbers for clarity):

```json
{"open":0.00000851,"high":0.000168,"low":0.00000851,"close":0.000106,"volume":121635.1,"quote_asset_volume":13.7082,"number_of_trades":561,"taker_buy_base_asset_volume":39139,"taker_buy_quote_asset_volume":4.5967,"open_time":1608872400000}
{"open":0.00010577,"high":0.000113,"low":0.00009122,"close":0.00010143,"volume":68514.3,"quote_asset_volume":6.9906,"number_of_trades":523,"taker_buy_base_asset_volume":10134,"taker_buy_quote_asset_volume":1.0316,"open_time":1608872460000}
{"open":0.00010182,"high":0.00011833,"low":0.00010148,"close":0.00011481,"volume":58331.5,"quote_asset_volume":6.4322,"number_of_trades":414,"taker_buy_base_asset_volume":7388.6,"taker_buy_quote_asset_volume":0.8015,"open_time":1608872520000}
```

And here is a sample output showing map-reduced statistics:

```json
{"dom_currency":"DOT","foreign_currency":"BUSD","avg_open":18.5642,"avg_low":18.5448,"avg_close":18.5640,"total_volume":1172410368}
{"dom_currency":"SNX","foreign_currency":"BUSD","avg_open":7.8312,"avg_low":7.8268,"avg_close":7.8310,"total_volume":204238512}
{"dom_currency":"DLT","foreign_currency":"ETH","avg_open":0.00038731,"avg_low":0.00038709,"avg_close":0.00038729,"total_volume":686501120}
```

## Benchmark Command Reference

```bash
# Basic usage with default parameters
uv run main.py

# Process files from a specific directory
uv run main.py -fx_path ./data/fx

# specify amount of benchmark runs for more reliable data
uv run main.py -runs 3

# Limit to specific number of files
uv run main.py -files 35

# Running specific benchmark sizes -- 35=1gb 160=5gb 320=10gb default=31gb
uv run main.py -files 35 -runs 3 
uv run main.py -files 160 -runs 3
uv run main.py -files 320 -runs 3
uv run main.py -runs 3
```

