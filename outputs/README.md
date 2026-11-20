# Generated evidence

The pipeline creates results from the selected dataset each time it runs.

Check `run_status.json` before interpreting results. A complete status with the same `run_id` as `reports/run_summary.json` identifies a successful run. Running/failed status, or a mismatched ID, means derived files may be incomplete or retained from an earlier run. The pipeline preserves screenshots and earlier event logs. It overwrites the known derived tables and reports during each successful run.

- `results/transaction_sales/` contains all retained transaction lines with INR `total_sales`.
- `results/data_quality_audit/` contains original rows after type conversion and rejection flags.
- `results/category_revenue/`, `top_five_products/`, and `location_revenue/` contain required aggregates.
- `results/sales_above_threshold/` contains lines strictly exceeding the configured INR threshold.
- `figures/` contains three charts from actual aggregate results.
- `reports/` contains schemas, query plans, lazy evaluation job IDs, measured RDD/DataFrame comparisons, run summary, and a generated explanation.
- `spark-events/` contains actual Spark event logs for later inspection in a Spark History Server.
- `screenshots/` is where the student saves actual Spark UI and DAG screenshots. Follow the instructions in that directory.

Each Spark CSV table is a directory of `part-*.csv` files with headers and a `_SUCCESS` marker. The union of part files is the complete table. File order across parts has no global guarantee, so sort by the documented key when reviewing a merged export. The output tables use Spark writers and can retain multiple partitions as data grows.
