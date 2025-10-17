# IoT Data Analyzer

## Environment
- **Python version:** 3.13.1

## Installation
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the CLI
```bash
python -m iotanalyzer.cli <input_csv>
```

### All options with examples
- `--start "2025-01-01 00:00:00"` – include records on/after this UTC timestamp
- `--end "2025-01-02 00:00:00"` – include records on/before this UTC timestamp
- `--site site_a site_b` – only include listed sites
- `--device device_1 device_2` – only include listed devices
- `--metric temperature humidity` – filter by metric (aliases accepted)
- `--statistic average` – compute statistics (repeatable, e.g. `--statistic population_stddev`)


### Example Commands
```bash
# Outputs the top 10 device + site + metric combinations by Highest average value and Highest variability (highest std dev)
python -m iotanalyzer.cli sample_data.csv \
    --statistic average --statistic population_stddev

# Expected output
Results for AverageStatistic
device_a_004/site_3 Pressure    =       103.26kPa
device_c_001/site_1 Pressure    =       102.55kPa
device_d_002/site_2 Pressure    =       102.43kPa
device_a_001/site_3 Pressure    =       102.05kPa
device_b_004/site_1 Pressure    =       101.66kPa
device_a_002/site_3 Pressure    =       101.51kPa
device_c_002/site_1 Pressure    =       100.78kPa
device_b_003/site_1 Pressure    =       100.43kPa
device_b_003/site_3 Pressure    =       99.02kPa
device_b_003/site_2 Pressure    =       98.91kPa 

Results for PopulationStandardDeviationStatistic
device_b_003/site_3 Humidity    =       18.28%RH
device_a_001/site_2 Humidity    =       16.15%RH
device_a_002/site_3 Humidity    =       14.93%RH
device_b_004/site_1 Humidity    =       14.12%RH
device_a_004/site_3 Humidity    =       14.09%RH
device_b_003/site_1 Humidity    =       13.19%RH
device_b_003/site_2 Humidity    =       11.74%RH
device_c_001/site_1 Humidity    =       11.59%RH
device_a_001/site_3 Humidity    =       11.39%RH
device_c_002/site_1 Humidity    =       11.34%RH


# Filter to site_1 temperatures in January and compute averages & stddev
python -m iotanalyzer.cli sample_data.csv \
    --start "2025-01-01 00:00:00" \
    --end "2025-01-31 23:59:59" \
    --site site_1 \
    --metric temperature \
    --statistic average --statistic population_stddev

# Example output
Results for AverageStatistic
device_b_004/site_1 Temperature =       25.97°C
device_c_001/site_1 Temperature =       20.26°C
device_b_003/site_1 Temperature =       20.23°C
device_c_002/site_1 Temperature =       18.56°C 

Results for PopulationStandardDeviationStatistic
device_b_003/site_1 Temperature =       4.49°C
device_c_001/site_1 Temperature =       4.49°C
device_b_004/site_1 Temperature =       2.72°C
device_c_002/site_1 Temperature =       2.14°C


# Anomaly counts
python -m iotanalyzer.cli sample_data.csv \
    --statistic anomaly_count

# Expected output
Results for AnomalyDetectionCountStatistic
device_a_002/site_3 Temperature =       16.00
device_b_003/site_2 Humidity    =       4.00
```

## Available Statistics
- `average` – mean value per `(site, device, metric)`.
- `min` – minimum value.
- `max` – maximum value.
- `count` – number of readings.
- `population_stddev` – population standard deviation (σ).
- `anomaly_count` – readings more than 3σ from the mean.

### Statistic Options
Statistics accept optional modifiers via `name:option=value` syntax. Options:

| Option | Values | Description |
|--------|--------|-------------|
| `sort` | `value_desc` (default), `value_asc`, `device_site_metric` | Controls ordering of output rows. |
| `k`    | integer ≥1, `all`, or empty | Limits output to top *k* entries after sorting (default 10). |

Examples:
```bash
--statistic average:sort=value_asc,k=5
--statistic population_stddev:k=all
--statistic anomaly_count:sort=device_site_metric,k=3
```

## Data Quality Considerations
- Timestamps must follow `YYYY-MM-DD HH:MM:SS` with `+0000 UTC` suffix.
- Metrics/units must map to known enums; unknown values raise parsing errors.
- Mixed units for the same `(site, device, metric)` combination are rejected to avoid skewed statistics.
- CSV rows are assumed well-formed (no missing columns); malformed rows will surface as exceptions.
- No missing values are assumed

## Handling Large Files
- CSV input is streamed row by row. No full in-memory load.
- Statistics operate in streaming fashion; only aggregates or small per-group summaries are maintained.
- Two passes are used for some statistics, which mean the file is iterated twice
