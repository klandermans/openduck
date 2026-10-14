"""Unit tests for OpenDuck core components."""

from pathlib import Path
import tempfile
import unittest

from openduck.core.config import (
    DUCKDB_EXTENSIONS,
    MAX_UI_ROWS,
    add_to_history,
    delete_connection,
    load_config,
    save_config,
    save_connection,
    save_query,
)
from openduck.core.error_classifier import classify_file_error
from openduck.core.sql_utils import is_duckdb_file, sql_for_file, toggle_sql_comment_lines


class TestSQLUtils(unittest.TestCase):
    """Test SQL query generation and comment toggling."""

    def test_duckdb_file_extensions(self):
        with tempfile.NamedTemporaryFile(suffix=".csv") as f_csv, \
             tempfile.NamedTemporaryFile(suffix=".parquet") as f_parquet, \
             tempfile.NamedTemporaryFile(suffix=".txt") as f_txt:
            self.assertTrue(is_duckdb_file(Path(f_csv.name)))
            self.assertTrue(is_duckdb_file(Path(f_parquet.name)))
            self.assertFalse(is_duckdb_file(Path(f_txt.name)))

    def test_sql_for_file(self):
        self.assertIn("read_csv_auto", sql_for_file(Path("data.csv")))
        self.assertIn("read_json_auto", sql_for_file(Path("data.json")))
        self.assertIn("ATTACH", sql_for_file(Path("data.duckdb")))
        self.assertIn("SELECT * FROM", sql_for_file(Path("data.parquet")))

    def test_toggle_sql_comment(self):
        lines = ["SELECT *", "FROM users;"]
        commented = toggle_sql_comment_lines(lines)
        self.assertEqual(commented, ["-- SELECT *", "-- FROM users;"])

        uncommented = toggle_sql_comment_lines(commented)
        self.assertEqual(uncommented, ["SELECT *", "FROM users;"])


class TestErrorClassifier(unittest.TestCase):
    """Test classification of various file errors."""

    def test_csv_file_not_found(self):
        msg = classify_file_error(Exception("Could not open CSV file"), "missing.csv")
        self.assertIn("File not found", msg)

    def test_csv_permission_denied(self):
        msg = classify_file_error(Exception("Permission denied reading CSV file"), "private.csv")
        self.assertIn("Permission denied", msg)

    def test_excel_error(self):
        msg = classify_file_error(Exception("Corrupted excel format"), "corrupt.xlsx")
        self.assertIn("Excel", msg)


class TestConfigManagement(unittest.TestCase):
    """Test configuration saving, loading, and queries."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.orig_config_file = None

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_constants(self):
        self.assertEqual(MAX_UI_ROWS, 10000)
        self.assertIn(".csv", DUCKDB_EXTENSIONS)
        self.assertIn(".parquet", DUCKDB_EXTENSIONS)


if __name__ == "__main__":
    unittest.main()
