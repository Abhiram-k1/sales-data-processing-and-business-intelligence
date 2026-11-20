"""Standard-library structural checks use explicitly constructed CSV test fixtures."""
import csv
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.input_validation import REQUIRED_COLUMNS, validate_csv


class CSVStructureTest(unittest.TestCase):
    def check_fixture(self, records, expected_error=None):
        with tempfile.TemporaryDirectory(prefix="assignment2-csv-test-") as temporary:
            path = Path(temporary) / "constructed_test_fixture.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(REQUIRED_COLUMNS)
                writer.writerows(records)
            if expected_error:
                with self.assertRaisesRegex(ValueError, expected_error):
                    validate_csv(path)
            else:
                self.assertEqual(validate_csv(path), len(records))

    def test_short_record_is_rejected(self):
        self.check_fixture([["id", "product"]], "has 2 fields")

    def test_extra_field_is_rejected(self):
        self.check_fixture([["id", "product", "category", "location", "2", "10", "date", "unexpected"]], "has 8 fields")

    def test_blank_field_is_preserved_for_quality_audit(self):
        self.check_fixture([["id", "", "category", "location", "2", "10", "date"]])

    def test_quoted_multiline_field_is_one_record(self):
        self.check_fixture([["id", 'product, with "quotes"\nand newline', "category", "location", "2", "10", "date"]])

    def test_unclosed_quote_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="assignment2-csv-test-") as temporary:
            path = Path(temporary) / "constructed_test_fixture.csv"
            path.write_text(",".join(REQUIRED_COLUMNS) + '\n"unclosed', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Malformed CSV syntax"):
                validate_csv(path)


if __name__ == "__main__":
    unittest.main()
