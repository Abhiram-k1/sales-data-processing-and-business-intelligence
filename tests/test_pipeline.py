"""Integration checks against the included real source sample, with an independent CSV oracle."""
import csv
import json
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.pipeline import run
from pyspark.sql import SparkSession
from src.analytics import REQUIRED, inspect_and_clean


def read_spark_csv(path):
    rows = []
    for part in sorted(path.glob("part-*.csv")):
        with part.open(newline="", encoding="utf-8") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


class SalesPipelineTest(unittest.TestCase):
    def test_real_sample_analytics_match_independent_oracle(self):
        source = ROOT / "data/sample/03_Dataset.csv"
        self.assertTrue(source.is_file(), "Run scripts/download_data.py to obtain the real sample.")
        with source.open(newline="", encoding="utf-8") as handle:
            source_rows = [{name: value.strip() for name, value in row.items()} for row in csv.DictReader(handle)]
        occurrences = defaultdict(int)
        for row in source_rows:
            occurrences[row["transaction_id"]] += 1
        category_totals = defaultdict(Decimal)
        location_totals = defaultdict(Decimal)
        quantities = defaultdict(int)
        eligible = []
        for row in source_rows:
            try:
                if any(not row[name].strip() for name in ["transaction_id", "product_name", "category", "store_location"]):
                    continue
                quantity = int(row["quantity_sold"])
                price = Decimal(row["unit_price"])
                datetime.strptime(row["transaction_date"], "%Y-%m-%d %H:%M:%S")
                if quantity <= 0 or price <= 0 or row.get("invoice_no", "").upper().startswith("C") or occurrences[row["transaction_id"]] > 1:
                    continue
                total = (quantity * price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                eligible.append((row["transaction_id"], total))
                category_totals[row["category"]] += total
                location_totals[row["store_location"]] += total
                quantities[row["product_name"]] += quantity
            except (ValueError, ArithmeticError):
                continue
        with tempfile.TemporaryDirectory(prefix="assignment2-test-") as temporary:
            output = Path(temporary)
            summary = run(source, output, partitions=2, repetitions=1)
            self.assertEqual(summary["input_rows"], len(source_rows))
            self.assertEqual(summary["valid_sales_rows"], len(eligible))
            actual_categories = {row["category"]: Decimal(row["total_revenue"]) for row in read_spark_csv(output / "results/category_revenue")}
            self.assertEqual(actual_categories, dict(category_totals))
            actual_locations = {row["store_location"]: Decimal(row["total_revenue"]) for row in read_spark_csv(output / "results/location_revenue")}
            self.assertEqual(actual_locations, dict(location_totals))
            actual_top = sorted([(row["product_name"], int(row["quantity_sold"])) for row in read_spark_csv(output / "results/top_five_products")], key=lambda item: (-item[1], item[0]))
            expected_top = sorted(quantities.items(), key=lambda item: (-item[1], item[0]))[:5]
            self.assertEqual(actual_top, expected_top)
            actual_above = {row["transaction_id"] for row in read_spark_csv(output / "results/sales_above_threshold")}
            self.assertEqual(actual_above, {identity for identity, total in eligible if total > Decimal("10000")})
            self.assertEqual(summary["lazy_demo_jobs_before_action"], [])
            self.assertTrue(summary["lazy_demo_jobs_after_action"])
            self.assertTrue((output / "figures/location_revenue.png").is_file())
            status = json.loads((output / "run_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "complete")
            self.assertEqual(status["run_id"], summary["run_id"])

    def test_invalid_quantity_and_cancellation_fixture(self):
        """Constructed test cases verify parsing rules. They are never assignment source data."""
        cases = [
            ("valid", "3", "500"), ("signed", "+2", "501"),
            ("fraction", "1.5", "502"), ("nan", "NaN", "503"),
            ("infinity", "Infinity", "504"), ("scientific", "1e2", "505"),
            ("overflow", "9223372036854775808", "506"),
            ("zero", "0", "507"), ("negative", "-2", "508"),
            ("lowercase_cancel", "4", "c509"),
        ]
        with tempfile.TemporaryDirectory(prefix="assignment2-parsing-test-") as temporary:
            source = Path(temporary) / "constructed_test_fixture.csv"
            with source.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=REQUIRED + ["invoice_no"])
                writer.writeheader()
                for identity, quantity, invoice in cases:
                    writer.writerow({
                        "transaction_id": identity, "product_name": "Test fixture product",
                        "category": "Test fixture category", "store_location": "Test fixture location",
                        "quantity_sold": quantity, "unit_price": "10.00",
                        "transaction_date": "2020-01-01 12:00:00", "invoice_no": invoice,
                    })
            spark = SparkSession.builder.master("local[2]").appName("Assignment2-QuantityRegressionTest") \
                .config("spark.sql.ansi.enabled", "false").config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate()
            try:
                _raw, audit, clean, quality = inspect_and_clean(spark, source)
                self.assertEqual({row.transaction_id for row in clean.collect()}, {"valid", "signed"})
                self.assertEqual(quality["flagged_by_rule"]["invalid_quantity"], 7)
                flags = {row.transaction_id: row for row in audit.collect()}
                self.assertTrue(flags["fraction"].invalid_quantity)
                self.assertEqual(flags["fraction"].quantity_sold_raw, "1.5")
                self.assertTrue(flags["lowercase_cancel"].cancellation)
            finally:
                spark.stop()

    def test_missing_input_marks_existing_output_as_failed(self):
        with tempfile.TemporaryDirectory(prefix="assignment2-failure-test-") as temporary:
            output = Path(temporary)
            (output / "run_status.json").write_text('{"status": "complete", "run_id": "old-run"}', encoding="utf-8")
            with self.assertRaises(FileNotFoundError):
                run(output / "missing.csv", output)
            status = json.loads((output / "run_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "failed")
            self.assertNotEqual(status["run_id"], "old-run")


if __name__ == "__main__":
    unittest.main()
