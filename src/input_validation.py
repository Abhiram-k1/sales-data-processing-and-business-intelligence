"""Streaming structural validation for the local CSV input."""
import csv

REQUIRED_COLUMNS = [
    "transaction_id", "product_name", "category", "store_location",
    "quantity_sold", "unit_price", "transaction_date",
]


def validate_csv(path):
    """Reject malformed CSV structure while allowing blank values for Spark's quality audit."""
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.reader(handle, strict=True)
            header = next(reader, None)
            if not header:
                raise ValueError("CSV is empty or has no header.")
            if any(not column for column in header) or len(set(header)) != len(header):
                raise ValueError("CSV header must contain unique, nonempty column names.")
            missing = sorted(set(REQUIRED_COLUMNS) - set(header))
            if missing:
                raise ValueError(f"CSV missing required columns {missing}. Run scripts/download_data.py or supply the documented schema.")
            count = 0
            for record_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise ValueError(f"CSV record {record_number} has {len(row)} fields. Header has {len(header)}. Correct the row width before running Spark.")
                count += 1
            if count == 0:
                raise ValueError("CSV has a header but contains no data records.")
            return count
    except csv.Error as error:
        raise ValueError(f"Malformed CSV syntax in {path}: {error}") from error
