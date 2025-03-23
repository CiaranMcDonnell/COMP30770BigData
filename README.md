# Big Data Processing Comparison

This project demonstrates and compares traditional single-machine data processing with distributed big data processing using Apache Spark. It processes financial exchange rate data from Parquet files, computing basic statistics while tracking memory usage and performance.

## Project Overview

The implementation includes two approaches:
- **Traditional approach**: Sequential processing using pandas on a single machine
- **Distributed approach**: Parallel processing using Spark's MapReduce paradigm

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
