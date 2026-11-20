# Sales Data Processing and Business Intelligence

Assignment 2 implements the screenshot's sales analysis with Apache Spark DataFrames and an equivalent RDD category-revenue calculation. A single command loads a real public dataset subset, inspects and cleans it, generates every required table, records execution plans and lazy evaluation evidence, compares results and measured execution times, and creates charts.

## Objective and problem statement

A retail organisation wants to analyse transaction records, identify products with strong sales, and compare regional performance. The required analytical operations are line sales, category revenue, the five best-selling products, location revenue, and transactions whose sales exceed ₹10,000. The screenshot also requires an explanation of Spark architecture, lazy evaluation, DataFrame optimization, RDD versus DataFrame processing, Spark UI, and DAG evidence.

The public UCI source provides online transactions. Country supplies the location grouping and StockCode prefix supplies the category grouping. The dataset has no physical store IDs or costs. These interpretations and their effect on the scenario are described in `data/README.md` and `docs/requirements.md`.

## Technologies and prerequisites

- Python 3.11 is the documented baseline. PySpark 3.5.3 requires a compatible Python runtime and Java.
- Java 17, with `JAVA_HOME` pointing to its installation and `java` available on PATH.
- PySpark 3.5.3 for all data processing, aggregation, output writing, plans, and RDD execution.
- openpyxl 3.1.5 reads the official Excel workbook during dataset acquisition.
- Matplotlib 3.9.2 plots computed aggregate values.
- At least 4 GB RAM is a practical local-development baseline. A Spark cluster is optional for distributed execution.
- Internet access is required for dependency installation and the original dataset download. Included subsets support offline data loading after setup.

## Project architecture

```text
Official UCI workbook
  -> scripts/download_data.py -> prepared INR sales CSV + source manifest + sample
  -> main.py -> pipeline/pipeline.py
  -> Spark CSV reader -> quality audit -> typed positive sales -> cached partitions
  -> DataFrame aggregates/filter/sort -> result tables + query plans
  -> equivalent RDD aggregate -> equality check + measured comparison
  -> aggregate charts + results explanation + event logs
```

```text
Assignment_2/
  main.py                    Single execution entry point
  requirements.txt           Pinned runtime dependencies
  Dockerfile                 Optional Linux runtime for Windows portability
  scripts/download_data.py   Official acquisition and transparent adaptation
  src/analytics.py           Quality rules, required analytics, RDD comparison
  src/input_validation.py    Streaming CSV structural preflight
  src/visualization.py       Charts from computed Spark aggregates
  pipeline/pipeline.py       Connected end-to-end orchestration
  data/README.md             Source, license, schema, subset and mapping details
  data/raw/README.md         Original workbook placement instructions
  data/processed/            Adapted real-record assignment subset and manifest
  data/sample/               Smaller real-record testing subset and manifest
  outputs/                   Generated tables, charts, reports, and Spark evidence
  docs/requirements.md       Screenshot transcription and requirement mapping
  docs/methodology.md        Algorithms, architecture and interpretation
  docs/validation.md         Verified checks and explicit testing limitations
  tests/test_pipeline.py     Real-sample integration test with independent oracle
  submission.md              Form fields for your own submission links
```

## Dataset

UCI **Online Retail** contains 541,909 real transaction lines with invoice, stock code, description, quantity, timestamp, unit price in GBP, customer ID, and country. The acquisition script selects the first 10,000 source rows for a compact assignment dataset and the first 500 for a testing sample. It preserves source invalid/missing/cancellation records for the audit. The original workbook is approximately 22.6 MB and is downloaded separately. CC BY 4.0 allows attributed adaptation and redistribution. Detailed field definitions and SHA-256 provenance are provided beside the CSVs. [UCI dataset](https://archive.ics.uci.edu/dataset/352/online+retail)

The prepared `unit_price` is in INR using an explicitly supplied constant. The default `--gbp-inr-rate 100` is illustrative. Original GBP values remain in the CSV. Regenerate with a suitable documented rate if you want a different conversion. Category is StockCode prefix and Store Location is Country. These are transparent proxies needed to adapt this public source to the assignment schema.

The packaged prepared CSV and sample CSV each contain **500 real rows** adapted from a publicly available first-500-row UCI mirror. Their manifests identify that CSV URL and its checksum. Running the default official-workbook acquisition expands the prepared CSV to 10,000 rows. The packaged subset supports immediate data loading and is below the form's size limit. [Packaged source mirror](https://raw.githubusercontent.com/rkharwar-nv/dataset_analysis/main/samples/uci-online-retail-sample.csv)

## Installation and environment setup

Open the extracted `Assignment_2` folder directly in your IDE. Run all commands from that folder.

```bash
python -m venv .venv
```

Activate the environment in Linux/macOS/WSL.

```bash
source .venv/bin/activate
```

Activate it in PowerShell.

```powershell
.venv\Scripts\Activate.ps1
```

Then install dependencies and verify Java.

```bash
python -m pip install -r requirements.txt
java -version
```

For Python worker consistency, use the same interpreter for the driver and workers. In PowerShell, set `$env:PYSPARK_PYTHON = (Resolve-Path .venv\Scripts\python.exe).Path` and `$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON`. In Linux/WSL, set `export PYSPARK_PYTHON="$PWD/.venv/bin/python"` and `export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"`.

## Execute the complete pipeline

Use the included real-record sample for a short development run.

```bash
python main.py --sample
```

Download or regenerate the 10,000-row prepared subset, then analyse it.

```bash
python scripts/download_data.py --max-rows 10000 --gbp-inr-rate 100
python main.py
```

If the full official workbook download is unavailable, regenerate the documented 500-row mirror subset with `python scripts/download_data.py --download-sample --max-rows 500`. An already downloaded source-record CSV can be adapted through `--source-csv PATH --source-csv-url ACTUAL_URL`. This mode records the CSV checksum and provenance independently of the workbook.

Keep the Spark UI open long enough to capture the screenshot deliverable.

```bash
python main.py --sample --ui-hold-seconds 300
```

Other configuration is exposed through `python main.py --help`. Examples include `--partitions 8`, `--threshold 10000`, `--benchmark-repetitions 3`, `--output PATH`, and `--input PATH`. Relative custom paths resolve from the current directory, while defaults resolve from the project folder. Each independent project carries its own source, dependencies, and configuration.

To convert the full source, use `python scripts/download_data.py --max-rows 0`. This produces a larger CSV and should be used separately from the below-10-MB submission subset.

For a Spark cluster, install compatible Spark 3.5.3 and Python dependencies on the relevant nodes, place the CSV in a path accessible to the driver and executors, and supply the appropriate `--master`. The packaged entry point accepts a local filesystem path, so a shared filesystem or mounted path is required. Spark's local mode executes partition tasks on local worker threads. A cluster distributes those tasks across executors.

## Optional Docker execution

Docker supplies a Linux runtime with Java 17 and Python 3.11, which avoids Windows Hadoop native-library setup.

```bash
docker build -t assignment2-sales .
docker run --rm -p 4040:4040 -v "${PWD}:/app" assignment2-sales python main.py --sample --ui-hold-seconds 300
```

The volume saves results into the extracted project. In PowerShell `${PWD}` expands the current directory. If Docker reports a bind-mount path problem, pass your absolute project path in the `-v` argument. Dependencies installed in the image remain available when the source directory is mounted. Acquire data through the same container if needed by replacing the command with `python scripts/download_data.py`.

## Pipeline and important source files

`scripts/download_data.py` downloads the original official archive, checks the workbook header, transforms actual rows, and writes provenance. `src/analytics.py` uses Spark expressions to trim strings, cast quantity/price/timestamp values, count missing data, mark invalid rows, reject duplicates and cancellations, and generate the required operations. Positive quantity and positive price define valid sales. It saves rejected records with audit flags for inspection.

`src/input_validation.py` streams the local CSV to check quoting, required unique headers, and record width before Spark loading. Blank fields proceed to the audit. Quantity must be signed whole-digit text fitting a positive 64-bit integer. Fractional and nonfinite values are rejected before casting, and cancellation prefixes are case-insensitive. The exact supported input format is documented in `data/README.md`.

`pipeline/pipeline.py` caches cleaned partitions, displays both input and typed schemas, saves each result table, records plans before and after an action, captures job IDs for the lazy query, measures equivalent RDD and DataFrame actions, and writes a run summary. `src/visualization.py` creates three charts from the actual aggregate rows. The chart view shows the largest 20 categories/locations, and the CSV outputs contain all groups.

Line total is `round(quantity_sold * unit_price, 2)`. Category/location revenue sums these rounded line totals. The best-selling definition is total positive quantity, with product name breaking ties. The high-value filter is strictly greater than ₹10,000. Revenue and units support product sales assessment. The source lacks cost data for calculating net profit.

## Expected outputs and locations

`outputs/results/` contains all retained line sales, the row-level quality audit, category revenue, top five products, location revenue, and high-value lines. Each table is a Spark CSV directory containing `part-*.csv` files. `outputs/figures/` contains category, market, and top-five-product charts. `outputs/reports/` contains schemas, every analytical plan, lazy evaluation evidence, actual RDD/DataFrame comparison timings, and the generated results explanation. `outputs/spark-events/` contains event logs. Exact values are calculated from the selected real data on execution.

Save actual Spark UI and DAG screenshots under `outputs/screenshots/` according to that folder's instructions. A History Server can inspect event logs after the application stops. Generated files are ignored by Git so you can choose which result evidence to commit before submission.

Each run updates `outputs/run_status.json` with a fresh run ID and running/complete/failed status. Results are authoritative when that status is complete and its ID matches `outputs/reports/run_summary.json`. A failed or interrupted rerun can leave partial or older derived files, which the status marker identifies as incomplete. Event logs and manually captured screenshots can include evidence from earlier runs.

## Validation

```bash
python -m compileall main.py src pipeline scripts tests
python -m unittest discover -s tests -v
```

The five structural CSV tests can run without Spark or Matplotlib installed through `python -m unittest discover -s tests -p test_input_validation.py -v`. Their constructed fixtures test malformed structure and are separate from the actual assignment dataset.

The integration test executes the normal pipeline against the included real sample and compares category/location totals, top-five product quantities, high-value IDs, valid-row counts, lazy job evidence, and chart creation against an independent standard-library CSV calculation. A separate explicitly constructed Spark fixture checks fractional/nonfinite/overflow quantity rejection and lower-case cancellation handling. Another test checks that missing input invalidates a previous output status. The pipeline also checks exact equality between the Spark DataFrame and RDD category totals. See `docs/validation.md` for checks actually performed during packaging and remaining environment limitations.

## Troubleshooting

- A missing CSV error gives the expected path. Run acquisition, supply `--input`, or use `--sample`.
- A download failure provides the official URL. Download `Online Retail.xlsx` manually to `data/raw/` and rerun acquisition.
- `JAVA_GATEWAY_EXITED` usually requires checking `JAVA_HOME`, Java 17, available memory, and the earliest Java error in the terminal.
- Python worker or version errors require `PYSPARK_PYTHON` to point to the same compatible interpreter used by the driver.
- Windows `winutils.exe`, `HADOOP_HOME`, or native Hadoop errors can affect Spark local file writes. Use Docker or WSL/Linux for the documented runtime, or configure the matching Hadoop native components for your installed Spark environment.
- An empty high-value result is valid for a small subset. Increase the real subset size or inspect the configured currency/rate and threshold.
- A zero-valid-sales error indicates that quality rules rejected every input record. Inspect the CSV schema and source data, and use the generated audit where available.
- Spark UI becomes unavailable after shutdown. Use `--ui-hold-seconds` during execution or inspect the saved event log in a History Server.
- The benchmark depends on local workload, Python overhead, cache/JIT effects, and action ordering. Inspect its reported timings and limitations before drawing performance conclusions.

## Submission

Read `docs/requirements.md` and fill the four screenshot form fields in `submission.md`. Create your own GitHub repository, upload your source ZIP and below-10-MB prepared CSV to Google Drive, and add accessible links. Include the analytical results, RDD/DataFrame discussion, and actual Spark UI/DAG screenshots in your submission evidence.
