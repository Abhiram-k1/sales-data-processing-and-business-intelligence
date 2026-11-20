"""CSV inspection, Spark DataFrame analytics, and equivalent RDD aggregation."""
import contextlib
import io
import json
import time
from decimal import Decimal

from pyspark.sql import functions as F
from src.input_validation import REQUIRED_COLUMNS, validate_csv

REQUIRED = REQUIRED_COLUMNS


def inspect_and_clean(spark, path):
    validate_csv(path)
    raw = spark.read.option("header", True).option("mode", "FAILFAST") \
        .option("multiLine", True).option("escape", '"').csv(str(path))
    missing_columns = sorted(set(REQUIRED) - set(raw.columns))
    if missing_columns:
        raise ValueError(f"CSV missing required columns {missing_columns}. Run scripts/download_data.py or supply the documented schema.")
    strings = raw.select(*[F.trim(F.col(c)).alias(c) for c in raw.columns])
    missing_counts = strings.agg(*[
        F.sum(F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)).alias(c)
        for c in REQUIRED
    ]).first().asDict()
    typed = strings.withColumn("quantity_sold_raw", F.col("quantity_sold")) \
        .withColumn("quantity_is_integer", F.coalesce(F.col("quantity_sold").rlike(r"^[+-]?[0-9]+$"), F.lit(False))) \
        .withColumn("quantity_sold", F.col("quantity_sold").cast("long")) \
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,4)")) \
        .withColumn("transaction_date", F.to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss"))
    duplicate_ids = strings.groupBy("transaction_id").count().filter("count > 1") \
        .select("transaction_id").withColumn("duplicate_id", F.lit(True))
    audited = typed.join(duplicate_ids, "transaction_id", "left")
    invalid_text = F.lit(False)
    for name in ["transaction_id", "product_name", "category", "store_location"]:
        invalid_text = invalid_text | F.col(name).isNull() | (F.col(name) == "")
    cancellation = F.upper(F.col("invoice_no")).startswith("C") if "invoice_no" in audited.columns else F.lit(False)
    audited = audited.withColumn("invalid_text", invalid_text) \
        .withColumn("invalid_quantity", ~F.col("quantity_is_integer") | F.col("quantity_sold").isNull() | (F.col("quantity_sold") <= 0)) \
        .withColumn("invalid_price", F.col("unit_price").isNull() | (F.col("unit_price") <= 0)) \
        .withColumn("invalid_date", F.col("transaction_date").isNull()) \
        .withColumn("cancellation", F.coalesce(cancellation, F.lit(False))) \
        .withColumn("duplicate_id", F.coalesce(F.col("duplicate_id"), F.lit(False)))
    flags = ["invalid_text", "invalid_quantity", "invalid_price", "invalid_date", "cancellation", "duplicate_id"]
    invalid = F.lit(False)
    for flag in flags:
        invalid = invalid | F.col(flag)
    audited = audited.withColumn("is_rejected", invalid)
    flag_counts = audited.agg(*[F.sum(F.col(c).cast("long")).alias(c) for c in flags]).first().asDict()
    clean = audited.filter(~F.col("is_rejected")).drop(*flags, "is_rejected", "quantity_is_integer", "quantity_sold_raw") \
        .withColumn("total_sales", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))
    return raw, audited, clean, {"missing_by_column": missing_counts, "flagged_by_rule": flag_counts}


def build_tables(sales, threshold):
    category = sales.groupBy("category").agg(F.sum("total_sales").alias("total_revenue")) \
        .orderBy(F.desc("total_revenue"), "category")
    products = sales.groupBy("product_name").agg(
        F.sum("quantity_sold").alias("quantity_sold"), F.sum("total_sales").alias("total_revenue")
    ).orderBy(F.desc("quantity_sold"), "product_name").limit(5)
    locations = sales.groupBy("store_location").agg(F.sum("total_sales").alias("total_revenue")) \
        .orderBy(F.desc("total_revenue"), "store_location")
    return {
        "transaction_sales": sales.orderBy("transaction_id"),
        "category_revenue": category,
        "top_five_products": products,
        "location_revenue": locations,
        "sales_above_threshold": sales.filter(F.col("total_sales") > F.lit(Decimal(str(threshold)))).orderBy(F.desc("total_sales"), "transaction_id"),
    }


def explain_text(frame, mode="formatted"):
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        frame.explain(mode=mode)
    return stream.getvalue()


def compare_rdd_dataframe(sales, repetitions=3):
    frame = sales.groupBy("category").agg(F.sum("total_sales").alias("revenue")).orderBy("category")
    rdd = sales.select("category", "total_sales").rdd.map(lambda row: (row[0], row[1])) \
        .reduceByKey(lambda left, right: left + right).sortByKey()
    frame_values = [(row[0], row[1]) for row in frame.collect()]
    rdd_values = rdd.collect()
    if frame_values != rdd_values:
        raise AssertionError("Equivalent RDD and DataFrame revenue aggregations disagree.")
    timings = []
    for repetition in range(repetitions):
        start = time.perf_counter()
        frame.collect()
        frame_seconds = time.perf_counter() - start
        start = time.perf_counter()
        rdd.collect()
        rdd_seconds = time.perf_counter() - start
        timings.append({"repetition": repetition + 1, "dataframe_seconds": frame_seconds, "rdd_seconds": rdd_seconds})
    return {
        "equivalent_results": True, "timings": timings,
        "benchmark_scope": "Cached cleaned input. Same category aggregation and key ordering. One warm-up each, then alternating DataFrame/RDD actions.",
        "limitations": "Small local input, Python worker overhead, cache/JIT/OS variability and fixed ordering influence timings. Results do not establish a universal speed claim.",
        "rdd_lineage": rdd.toDebugString().decode("utf-8"),
        "category_totals_inr": [{"category": category, "revenue": str(value)} for category, value in frame_values],
    }


def save_json(path, values):
    path.write_text(json.dumps(values, indent=2, default=str), encoding="utf-8")
