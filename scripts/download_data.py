"""Download UCI Online Retail and create a documented real-record CSV subset."""
import argparse
import csv
import hashlib
import json
import urllib.request
import zipfile
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
SOURCE_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
DATASET_URL = "https://archive.ics.uci.edu/dataset/352/online+retail"
SAMPLE_URL = "https://raw.githubusercontent.com/rkharwar-nv/dataset_analysis/main/samples/uci-online-retail-sample.csv"
SOURCE_FIELDS = ("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country")
FIELDS = [
    "transaction_id", "product_name", "category", "store_location",
    "quantity_sold", "unit_price", "transaction_date", "invoice_no",
    "stock_code", "unit_price_gbp", "gbp_inr_rate", "source_excel_row",
]


def convert_row(row, excel_row, rate):
    invoice, stock, description, quantity, date, price, _customer, country = row[:8]
    stock_text = "" if stock is None else str(stock).strip()
    try:
        price_gbp = Decimal(str(price))
        price_inr = format((price_gbp * rate).quantize(Decimal("0.0001")), "f")
    except (InvalidOperation, TypeError):
        price_inr = ""
    return dict(zip(FIELDS, [
        f"{invoice}-{excel_row}", description or "",
        f"Stock prefix {stock_text[:2]}" if stock_text else "",
        country or "", quantity if quantity is not None else "", price_inr,
        date.strftime("%Y-%m-%d %H:%M:%S") if isinstance(date, datetime) else str(date or ""),
        invoice or "", stock_text, price if price is not None else "",
        format(rate, "f"), excel_row,
    ]))


def write_adapted(rows, max_rows, sample_rows, rate, provenance):
    processed = ROOT / "data/processed"
    sample = ROOT / "data/sample"
    processed.mkdir(parents=True, exist_ok=True)
    sample.mkdir(parents=True, exist_ok=True)
    count = 0
    sample_count = 0
    with (processed / "03_Dataset.csv").open("w", newline="", encoding="utf-8") as full_file, (sample / "03_Dataset.csv").open("w", newline="", encoding="utf-8") as sample_file:
        writer = csv.DictWriter(full_file, fieldnames=FIELDS)
        sample_writer = csv.DictWriter(sample_file, fieldnames=FIELDS)
        writer.writeheader()
        sample_writer.writeheader()
        for excel_row, row in enumerate(rows, start=2):
            if max_rows and count >= max_rows:
                break
            converted = convert_row(row, excel_row, rate)
            writer.writerow(converted)
            if count < sample_rows:
                sample_writer.writerow(converted)
                sample_count += 1
            count += 1
    manifest = {
        "dataset_url": DATASET_URL, "original_download_url": SOURCE_URL,
        "license": "CC BY 4.0", **provenance,
        "max_rows": max_rows, "rows_written": count, "sample_rows_written": sample_count,
        "gbp_inr_rate": str(rate), "exchange_rate_kind": "User-supplied illustrative constant, not historical market FX",
        "category_mapping": "First two characters of StockCode, prefixed Stock prefix",
        "location_mapping": "Country (sales market proxy for Store Location)",
        "source_fields_preserved": ["invoice_no", "stock_code", "unit_price_gbp", "source_excel_row"],
    }
    (processed / "acquisition_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (sample / "acquisition_manifest.json").write_text(json.dumps({**manifest, "rows_written": sample_count}, indent=2), encoding="utf-8")
    print(f"Created {processed / '03_Dataset.csv'} with {count} real source rows.")
    print(f"Created {sample / '03_Dataset.csv'} with {sample_count} real source rows.")
    if (processed / "03_Dataset.csv").stat().st_size >= 10_000_000:
        print("Prepared CSV is >=10 MB. Choose a smaller --max-rows for the submission upload.")


def acquire(max_rows=10000, sample_rows=500, gbp_inr_rate="100", workbook=None, source_csv=None, source_csv_url=None, download_sample=False):
    rate = Decimal(gbp_inr_rate)
    if not rate.is_finite() or rate <= 0:
        raise ValueError("GBP/INR rate must be a finite positive number.")
    if max_rows < 0 or sample_rows < 1:
        raise ValueError("max-rows must be >=0 (0 means full data), sample-rows must be >=1.")
    raw = ROOT / "data/raw"
    raw.mkdir(parents=True, exist_ok=True)
    if download_sample:
        source_csv = raw / "online_retail_first500.csv"
        urllib.request.urlretrieve(SAMPLE_URL, source_csv)
        source_csv_url = SAMPLE_URL
    if source_csv:
        csv_path = Path(source_csv).expanduser().resolve()
        if not csv_path.is_file():
            raise FileNotFoundError(f"Source CSV missing at {csv_path}")
        with csv_path.open(newline="", encoding="utf-8-sig") as handle:
            rows = csv.reader(handle)
            header = next(rows)
            if tuple(header) != SOURCE_FIELDS:
                raise ValueError(f"Unexpected source CSV columns {header}. Expected {SOURCE_FIELDS}.")
            write_adapted(rows, max_rows, sample_rows, rate, {
                "source_kind": "Actual UCI source-record CSV subset",
                "source_csv_url": source_csv_url or "User-provided source CSV; provide its public URL with --source-csv-url",
                "source_csv_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
                "selection": "First available input CSV data rows in supplied source order. Packaged mirror contains the first 500 original UCI rows.",
                "source_excel_row_interpretation": "Input CSV row index including header; matches original worksheet row for the documented first-500-row mirror.",
            })
        return
    workbook_path = Path(workbook).expanduser().resolve() if workbook else raw / "Online Retail.xlsx"
    if not workbook_path.exists():
        archive_path = raw / "online_retail.zip"
        temporary = archive_path.with_suffix(".download")
        try:
            urllib.request.urlretrieve(SOURCE_URL, temporary)
            with zipfile.ZipFile(temporary) as archive:
                members = [n for n in archive.namelist() if n.endswith("Online Retail.xlsx")]
                if len(members) != 1:
                    raise ValueError("UCI archive did not contain the expected Online Retail.xlsx.")
                workbook_path.write_bytes(archive.read(members[0]))
            temporary.replace(archive_path)
        except Exception as error:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"Download failed. Download {SOURCE_URL} manually and place Online Retail.xlsx in data/raw, or use --workbook. Details: {error}") from error
    book = load_workbook(workbook_path, read_only=True, data_only=True)
    sheet = book.active
    rows = sheet.iter_rows(values_only=True)
    header = next(rows)
    if tuple(header[:8]) != SOURCE_FIELDS:
        book.close()
        raise ValueError(f"Unexpected workbook columns {header}. Expected {SOURCE_FIELDS}.")
    try:
        write_adapted(rows, max_rows, sample_rows, rate, {
            "source_kind": "Official complete UCI workbook",
            "download_url": SOURCE_URL,
            "workbook_sha256": hashlib.sha256(workbook_path.read_bytes()).hexdigest(),
            "selection": "First worksheet data rows in original order, including source missing/invalid/return records",
        })
    finally:
        book.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=10000, help="First source rows to convert. 0 converts the full workbook.")
    parser.add_argument("--sample-rows", type=int, default=500)
    parser.add_argument("--gbp-inr-rate", default="100", help="Explicit illustrative GBP to INR multiplier.")
    parser.add_argument("--workbook", help="Use an already downloaded official workbook.")
    parser.add_argument("--source-csv", help="Convert a real UCI CSV subset with the original eight columns.")
    parser.add_argument("--source-csv-url", help="Record the actual source CSV URL in the manifest.")
    parser.add_argument("--download-sample", action="store_true", help="Download and adapt the documented public first-500-row UCI mirror.")
    args = parser.parse_args()
    if sum(bool(value) for value in [args.workbook, args.source_csv, args.download_sample]) > 1:
        parser.error("Choose --workbook, --source-csv, or --download-sample.")
    acquire(args.max_rows, args.sample_rows, args.gbp_inr_rate, args.workbook, args.source_csv, args.source_csv_url, args.download_sample)


if __name__ == "__main__":
    main()
