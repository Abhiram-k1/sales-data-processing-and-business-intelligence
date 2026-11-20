# Dataset and provenance

The project uses **Online Retail**, published by Daqing Chen in the UCI Machine Learning Repository. The original workbook contains 541,909 transaction lines from a UK online retailer during 1 December 2010 through 9 December 2011. UCI lists the dataset under CC BY 4.0, permitting attributed sharing and adaptation.

- Dataset page https://archive.ics.uci.edu/dataset/352/online+retail
- Download https://archive.ics.uci.edu/static/public/352/online+retail.zip
- Citation Chen, D. (2015). Online Retail [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33
- License https://creativecommons.org/licenses/by/4.0/

Run `python scripts/download_data.py`. It obtains `Online Retail.xlsx` in `data/raw/`, reads the original rows with openpyxl, writes the first 10,000 source data rows to `data/processed/03_Dataset.csv`, and writes the first 500 to `data/sample/03_Dataset.csv`. Both retain original row order and observed missing, invalid, and cancellation records. The source archive and workbook are excluded from packaging. A supplied official workbook can be converted with `--workbook PATH`.

The prepared dataset is a declared subset, and the sample is a smaller declared subset. Neither represents the full original workbook. The manifest beside each CSV records source URLs, SHA-256 of the source workbook, row counts, selection rule, and transformation assumptions. `--max-rows 0` converts the complete source. `--max-rows N` changes the submission subset, and `--sample-rows N` changes the testing subset.

The packaged `data/processed/03_Dataset.csv` and `data/sample/03_Dataset.csv` each contain **500 actual source rows** from a verified public mirror of the first 500 UCI records. The mirror URL is https://raw.githubusercontent.com/rkharwar-nv/dataset_analysis/main/samples/uci-online-retail-sample.csv. The manifests record `source_csv_sha256` and the mirror URL. The official workbook was not used to generate this packaged subset, so the manifests make no workbook checksum claim. The default acquisition command downloads the official workbook and then expands the prepared subset to 10,000 records.

Run `python scripts/download_data.py --download-sample --max-rows 500` to reproduce the mirrored subset. Alternatively, use `--source-csv PATH --source-csv-url ACTUAL_URL` with a downloaded real-record CSV containing the original eight source columns. The provided mirror preserves the first original rows, so `source_excel_row` matches the original worksheet position. For another supplied CSV, it identifies that CSV's row position and may require an external mapping to the original worksheet.

The screenshot specifies seven attributes. The source supports five directly and requires documented interpretations for category and store location.

| CSV column | Type | Meaning and source |
| --- | --- | --- |
| transaction_id | string | Unique source line key made from InvoiceNo and Excel row number. One invoice may contain multiple product lines. |
| product_name | string | Original Description. |
| category | string | `Stock prefix XX` using the first two StockCode characters. This is a deterministic grouping proxy. |
| store_location | string | Original Country, representing the sales market. Physical stores are absent from the source. |
| quantity_sold | integer | Original Quantity. Negative values are retained during acquisition and rejected by the positive-sales policy. |
| unit_price | decimal | Original GBP UnitPrice multiplied by the chosen GBP/INR rate, rounded to four decimals. |
| transaction_date | timestamp text | Original InvoiceDate in `yyyy-MM-dd HH:mm:ss` form. |
| invoice_no | string | Original InvoiceNo, used to identify cancellations beginning with C. |
| stock_code | string | Original product identifier. |
| unit_price_gbp | decimal | Unconverted original UnitPrice. |
| gbp_inr_rate | decimal | Explicit conversion multiplier used by acquisition. |
| source_excel_row | integer | Source worksheet row number including the header. |

The default multiplier is **100 INR per GBP**, an illustrative configurable value. It is supplied to make the screenshot's INR threshold executable and has no claim to historical exchange rates. Use `python scripts/download_data.py --gbp-inr-rate YOUR_RATE` to regenerate the CSV. All analysis uses the prepared INR `unit_price`, and `unit_price_gbp` remains available for verification.

The source supplies no purchase cost or product margin. Revenue and quantity rankings address product sales performance. Net profit cannot be computed from this dataset. If your instructor requires physical stores, merchandising categories, or measured profit, provide a real CSV with those attributes through `--input` and retain its provenance.

## Supported input CSV format

Input must be a local UTF-8 comma-separated CSV with a header containing the exact seven required snake_case names above. Headers must be unique and nonempty. Additional source metadata columns are permitted. Every record must have the same number of fields as its header. Standard double-quoted fields, doubled embedded quotes, and quoted multiline text are supported. A streaming standard-library preflight rejects malformed quoting and short/extra-width rows before Spark loading. Blank field values remain available for Spark's missing/invalid audit.

Quantity text is trimmed and must match `[+-]?[0-9]+`. Positive values must fit the signed 64-bit integer range, with maximum 9,223,372,036,854,775,807. Fractional strings such as `1.5`, scientific notation, NaN/Infinity, out-of-range values, zero, and negatives are rejected. The audit retains the original trimmed value as `quantity_sold_raw` so a failed cast remains explainable. Price is parsed as `decimal(18,4)` and must remain positive after parsing. Timestamp text uses the exact timezone-free format `yyyy-MM-dd HH:mm:ss`. The prepared `unit_price` contract is INR. The optional `invoice_no` column marks cancellation records when its trimmed value starts with C, case-insensitively.

For the form's **Dataset Upload (Below 10 MB)** field, upload `data/processed/03_Dataset.csv`, verify its size is below 10,000,000 bytes, and describe the row selection recorded in its manifest. The packaged dataset is the adapted first-500-row mirror subset. After default official-workbook acquisition, it becomes the first-10,000-row adapted subset. The original 22.6 MB workbook exceeds that field's limit. Keep the original UCI download URL in the separate Dataset Link field.
