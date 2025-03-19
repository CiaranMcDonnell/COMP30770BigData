Title: Quick Access of Related Data for FX Calibration

Ciaran XXXXX Imrane AMRI 22412352

<github link>

# Dataset
Our dataset consists of two meaningful components:

- FX Quotes. These tell us the exchange rate from a currency to another. We use open (beginning of day) for stability
- TS. Tells us movement of interest rate for that currency based on an index

volume

variety 


# Objective

Mapping data in finance is often a bottleneck for calibration. We aim to simplify this process by offloading the heavy
thinking to Spark. With this proof of concept, we aim to show a scalable solution to the century old problem of
relating large financial datasets across each other at dates/tenors.

# Trad. solution


use pandas to read the csvs
```python
    fx_data = pd.read_csv(fx_file)
    ts_data = pd.read_csv(ts_file)
```

merge data with pandas utility at date
```python
    joined_data = pd.merge(fx_data, ts_data, on="Date", how="inner")
```

merge columns of term structure data into new column, i.e. (1w, 1m, 3m, ...) -> ([1w, 1m, 3m, ...])
```python
    joined_data["TS"] = joined_data[["1w", "1m", "3m", "6m", "12m"]].apply(
        lambda row: row.tolist(), axis=1
    )
```

Finally we remap some keys for ease of access later


# Map reduce

