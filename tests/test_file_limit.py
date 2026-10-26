"""Tests for file limit raising and cursor closing."""

import asyncio
import os
import resource
import tempfile
import unittest
from pathlib import Path
import duckdb

from openduck.main import _increase_file_limit
from openduck.gui.app import DuckCLI
from openduck.gui.widgets.query_tab import QueryTab
from openduck.gui.widgets.sql_editor import SqlTextArea
from textual.widgets import TabbedContent


class TestFileLimitAndCursorClosing(unittest.IsolatedAsyncioTestCase):
    """Verify that file limits are raised and cursors are properly closed."""

    def test_increase_file_limit(self):
        """_increase_file_limit raises RLIMIT_NOFILE soft limit."""
        soft_before, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        _increase_file_limit()
        soft_after, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        self.assertGreaterEqual(soft_after, soft_before)

    async def test_duckdb_cursor_closed_after_query(self):
        """DuckDB cursor is closed after running a query on a Parquet file."""
        # Create a parquet file with multiple rows
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            con = duckdb.connect()
            con.execute(f"COPY (SELECT range AS id, random() AS val FROM range(20000)) TO '{tmp_path}' (FORMAT PARQUET)")
            con.close()

            app = DuckCLI()
            async with app.run_test() as pilot:
                await asyncio.sleep(0.3)
                await pilot.pause()

                # Open a tab and run a query
                worker = app.action_new_tab()
                await worker.wait()
                await pilot.pause()

                tabs = app.query_one("#tabs", TabbedContent)
                pane = app.query_one(f"#{tabs.active}")
                tab = pane.query_one(QueryTab)
                editor = tab.query_one(SqlTextArea)
                editor.text = f"SELECT * FROM '{tmp_path}'"

                # Run query
                await app.action_run_query()
                await pilot.pause()

                # Check that the data loaded
                self.assertIsNotNone(tab.full_data)
                self.assertGreater(len(tab.full_data), 0)

                # Run lsof to verify that tmp_path is NOT held open
                import subprocess
                res = subprocess.check_output(["lsof", "-p", str(os.getpid())], text=True)
                open_matching = [line for line in res.splitlines() if tmp_path in line]
                self.assertEqual(len(open_matching), 0, f"File still held open: {open_matching}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    unittest.main()
