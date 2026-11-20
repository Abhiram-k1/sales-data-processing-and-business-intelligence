"""End-to-end source loading, quality audit, sales analytics, and Spark evidence."""
import argparse
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.analytics import build_tables, compare_rdd_dataframe, explain_text, inspect_and_clean, save_json
from src.visualization import save_charts

ROOT = Path(__file__).resolve().parents[1]


def run(input_path, output_path, master="local[2]", partitions=4, threshold="10000", repetitions=3, ui_hold_seconds=0):
    input_path = Path(input_path).expanduser().resolve()
    output_path = Path(output_path).expanduser().resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    status_path = output_path / "run_status.json"
    status = {
        "run_id": str(uuid.uuid4()), "status": "running", "input": str(input_path),
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_policy": "Results are authoritative only when this status is complete and run_summary.json has the same run_id. Failed or interrupted runs may leave partial or older files.",
    }
    save_json(status_path, status)
    spark = None
    try:
        if not input_path.is_file():
            raise FileNotFoundError(f"Dataset missing at {input_path}. Run python scripts/download_data.py, or use --sample with the included real-record subset.")
        for name in ["results", "figures", "reports", "spark-events"]:
            (output_path / name).mkdir(parents=True, exist_ok=True)
        spark = SparkSession.builder.appName("Assignment2-SalesDataFrameAnalytics").master(master) \
            .config("spark.sql.shuffle.partitions", str(partitions)) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.ansi.enabled", "false") \
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", (output_path / "spark-events").as_uri()) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        raw, audit, sales, quality = inspect_and_clean(spark, input_path)
        print("Input schema")
        raw.printSchema()
        print("Typed sales schema")
        sales.printSchema()
        raw.show(20, truncate=False)
        (output_path / "reports/input_schema.json").write_text(raw.schema.json(), encoding="utf-8")
        (output_path / "reports/sales_schema.json").write_text(sales.schema.json(), encoding="utf-8")
        raw_count = raw.count()
        sales = sales.repartition(partitions).cache()
        valid_count = sales.count()
        save_json(output_path / "reports/data_quality.json", {"input_rows": raw_count, "valid_sales_rows": valid_count, **quality})
        audit.write.mode("overwrite").option("header", True).csv(str(output_path / "results/data_quality_audit"))
        if valid_count == 0:
            raise ValueError("No valid positive sales remain. Check outputs/reports and the documented input schema.")
        tables = build_tables(sales, threshold)
        spark.sparkContext.setJobGroup("lazy-demo", "Build transformations, then trigger one action")
        lazy_query = sales.filter(F.col("total_sales") > F.lit(0)).select("category", "total_sales") \
            .groupBy("category").agg(F.sum("total_sales").alias("revenue")).orderBy("category")
        before = list(spark.sparkContext.statusTracker().getJobIdsForGroup("lazy-demo"))
        (output_path / "reports/lazy_query_plan_before.txt").write_text(explain_text(lazy_query, "extended"), encoding="utf-8")
        lazy_query.collect()
        after = list(spark.sparkContext.statusTracker().getJobIdsForGroup("lazy-demo"))
        (output_path / "reports/lazy_query_plan_after.txt").write_text(explain_text(lazy_query, "formatted"), encoding="utf-8")
        spark.sparkContext.setJobGroup("analytics", "Required DataFrame outputs")
        counts = {}
        for name, table in tables.items():
            print(f"\n{name}")
            table.show(20, truncate=False)
            table.write.mode("overwrite").option("header", True).csv(str(output_path / "results" / name))
            counts[name] = table.count()
            (output_path / "reports" / f"{name}_execution_plan.txt").write_text(explain_text(table, "extended"), encoding="utf-8")
        comparison = compare_rdd_dataframe(sales, repetitions)
        save_json(output_path / "reports/rdd_dataframe_comparison.json", comparison)
        save_charts(tables, output_path / "figures")
        summary = {
            "run_id": status["run_id"],
            "input": str(input_path), "input_rows": raw_count, "valid_sales_rows": valid_count,
            "rejected_rows": raw_count - valid_count, "quality": quality,
            "output_row_counts": counts, "strict_threshold_inr": str(threshold),
            "currency": "INR from the CSV acquisition rate. Original GBP price retained where supplied.",
            "lazy_demo_jobs_before_action": before, "lazy_demo_jobs_after_action": after,
            "spark_version": spark.version, "master": master, "partitions": partitions,
            "spark_ui": spark.sparkContext.uiWebUrl,
            "interpretation": "Revenue and quantity describe the selected source rows. Category and location are documented proxies. Cost data is unavailable for net profit.",
        }
        save_json(output_path / "reports/run_summary.json", summary)
        report = (
            f"# Computed results\n\nInput contained {raw_count} rows. The positive-sales policy retained {valid_count} and rejected {raw_count-valid_count}. "
            f"The strict INR threshold is {threshold}. See run_summary.json for rule counts and table sizes.\n\n"
            "Revenue is the sum of rounded line totals under the configured GBP/INR conversion. "
            "Top five products are ranked by total positive units, with product name resolving ties. "
            "Category represents stock-code prefix and location represents country. "
            "The source provides no cost or physical store identifier. Revenue rankings support sales analysis and cannot establish net profit.\n\n"
            "RDD and DataFrame totals were compared exactly. See rdd_dataframe_comparison.json for actual timings and limitations. "
            "The lazy-query plans and job IDs record the action boundary. Spark event logs support later DAG inspection in the History Server.\n"
        )
        (output_path / "reports/results_explanation.md").write_text(report, encoding="utf-8")
        print(f"\nSaved results to {output_path}")
        if ui_hold_seconds:
            print(f"Keeping Spark UI active for {ui_hold_seconds} seconds for screenshot capture.")
            time.sleep(ui_hold_seconds)
        status.update({"status": "complete", "finished_at_utc": datetime.now(timezone.utc).isoformat(), "summary": "reports/run_summary.json"})
        save_json(status_path, status)
        return summary
    except BaseException as error:
        status.update({"status": "failed", "finished_at_utc": datetime.now(timezone.utc).isoformat(), "error": f"{type(error).__name__}: {error}"})
        save_json(status_path, status)
        raise
    finally:
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="CSV path. Defaults to data/processed/03_Dataset.csv.")
    parser.add_argument("--sample", action="store_true", help="Use the real-record sample CSV.")
    parser.add_argument("--output", type=Path, default=ROOT / "outputs")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--threshold", default="10000", help="Strict line total threshold in INR.")
    parser.add_argument("--benchmark-repetitions", type=int, default=3)
    parser.add_argument("--ui-hold-seconds", type=int, default=0)
    args = parser.parse_args()
    if args.partitions < 1 or args.benchmark_repetitions < 1 or args.ui_hold_seconds < 0:
        parser.error("Partitions and repetitions must be positive and hold seconds must be nonnegative.")
    if args.input and args.sample:
        parser.error("Choose --input or --sample.")
    try:
        threshold = Decimal(args.threshold)
        if not threshold.is_finite() or threshold < 0:
            parser.error("Threshold must be a finite nonnegative INR amount.")
    except InvalidOperation:
        parser.error("Threshold must be a numeric INR amount.")
    source = args.input or ROOT / ("data/sample/03_Dataset.csv" if args.sample else "data/processed/03_Dataset.csv")
    run(source, args.output, args.master, args.partitions, args.threshold, args.benchmark_repetitions, args.ui_hold_seconds)


if __name__ == "__main__":
    main()
