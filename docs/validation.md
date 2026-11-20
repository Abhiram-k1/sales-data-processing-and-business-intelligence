# Validation status

This file records verified checks at packaging time. Runtime results and measurements are produced only by an actual execution.

## Available verification commands

```bash
python -m compileall main.py src pipeline scripts tests
python -m unittest discover -s tests -v
python main.py --sample
```

The integration test uses the real sample and an independent standard-library CSV oracle. It checks retained row count, category and location decimal totals, top-five quantities, strict high-value membership, lazy action job IDs, and chart output. The normal pipeline checks DataFrame/RDD aggregation equality before recording actual timings.

## Packaging checks

The following checks were completed during project creation.

- Python 3.12.14 syntax compilation passed for `main.py`, `src/`, `pipeline/`, `scripts/`, and `tests/` using `compileall`.
- The acquisition adapter executed successfully on the verified 500-row public UCI mirror CSV. It created both prepared/sample CSVs and explicit CSV-source provenance manifests.
- The packaged manifests record the source CSV checksum, source URL, actual 500-row selection, and the illustrative conversion assumption. The original workbook was not used for this packaged subset.
- Five standard-library CSV structural tests passed after final review. They cover short rows, extra-width rows, blank field preservation, standard quoted multiline fields, and malformed quoting. Run them with `python -m unittest discover -s tests -p test_input_validation.py -v`.

Final review added strict whole-digit quantity parsing before Spark's long cast, case-insensitive cancellation detection, and a run-status marker that invalidates authoritative results after a failed rerun. A targeted Spark regression fixture covers fractional, scientific, nonfinite, overflow and nonpositive quantities, plus lower-case cancellation. These Spark checks remain unexecuted until a Spark runtime is available.

The bundled runtime initially had neither PySpark nor Matplotlib installed. Spark pipeline execution and the integration test were therefore pending at this stage. The parent packaging task may append subsequent successful execution or precise failure details here. Archive integrity and source inclusion are checked independently during final packaging.

## Infrastructure and manual evidence

Spark requires Java and a compatible Python runtime. Windows may require matching Hadoop native components for local file output, or a Docker/WSL/Linux runtime. The Dockerfile supplies Java 17 and Python 3.11. Cluster execution requires a real Spark cluster and shared input access.

Spark UI and DAG screenshots must be captured from an actual running UI or History Server. Follow `outputs/screenshots/README.md`. A supplied screenshot should be listed here with the run/input and captured page. No timing, chart, cluster result, or screenshot is considered verified until the corresponding run or capture actually occurs.

## Final coordinator checks

On 20 November 2026 under Python 3.12.14, `python -m unittest discover -s tests -p test_input_validation.py -v` passed all five structural CSV tests. An independent standard-library comparison also verified all 500 prepared sample records against the acquired source CSV for IDs, descriptions, stock codes, grouping fields, quantities, dates and the documented currency conversion.

The coordinator could not obtain Spark/Scala dependencies. Shell downloads failed with Windows socket permission error 10013, and the browser runtime download was blocked by its client. The full Spark pipeline, Spark regression/integration tests, chart generation, execution timings, UI/DAG screenshots and Docker/cluster execution remain unverified. Capture the required UI/DAG screenshots after running the documented command in your own runtime.

## Publication preparation checks

On 20 November 2026, Python 3.12.14 parsed all 10 Python source and test files without syntax errors. `python -s -m unittest discover -s tests -p test_input_validation.py -v` passed 5 tests against the publication project.

The submitted `03_Dataset.csv` contains 500 records and is 63,971 bytes, below 10 MB. Its bytes match the standalone dataset prepared for Drive upload. The project input path and documented filenames were checked after renaming. Spark execution and its infrastructure checks retain the unverified status documented above.
