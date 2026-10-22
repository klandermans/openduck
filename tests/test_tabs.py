"""Unit tests for multi-tab behavior, Ctrl+T, and file opening tabs."""

import asyncio
from pathlib import Path
import tempfile
import unittest

from textual.widgets import TabbedContent
from openduck.gui.app import DuckCLI
from openduck.gui.widgets.query_tab import QueryTab
from openduck.gui.widgets.sql_editor import SqlTextArea


class TestTabsAndShortcuts(unittest.IsolatedAsyncioTestCase):
    """Test suite for opening files into tabs, Ctrl+T new tab, and tab management."""

    async def test_action_new_tab_creates_empty_tab(self):
        """DuckCLI.action_new_tab() creates a new empty tab with 'New Tab' as base name."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)
            initial_count = tabs.tab_count

            # Trigger action_new_tab
            worker = app.action_new_tab()
            await worker.wait()
            await pilot.pause()

            # Verify tab count increased by 1
            self.assertEqual(tabs.tab_count, initial_count + 1)

            # Verify active tab is the new tab
            active_id = tabs.active
            self.assertIsNotNone(active_id)
            self.assertEqual(app._tab_base_names.get(active_id), "New Tab")

            # Verify the editor has empty SQL
            pane = app.query_one(f"#{active_id}")
            qtab = pane.query_one(QueryTab)
            editor = qtab.query_one(SqlTextArea)
            self.assertEqual(editor.text.strip(), "")

            # Verify tab label indicates done status
            tab_widget = tabs.get_tab(active_id)
            self.assertIn("New Tab", tab_widget.label_text)
            self.assertIn("done", tab_widget.label_text)

    async def test_ctrl_t_shortcut_creates_empty_tab(self):
        """Pressing Ctrl+T creates a new empty tab."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)
            initial_count = tabs.tab_count

            # Press Ctrl+T
            await pilot.press("ctrl+t")
            await asyncio.sleep(0.1)
            await pilot.pause()

            self.assertEqual(tabs.tab_count, initial_count + 1)
            active_id = tabs.active
            self.assertEqual(app._tab_base_names.get(active_id), "New Tab")
            pane = app.query_one(f"#{active_id}")
            editor = pane.query_one(SqlTextArea)
            self.assertEqual(editor.text.strip(), "")

    async def test_open_file_replaces_empty_unsaved_tab(self):
        """Opening a file when the active tab is empty and unsaved replaces that tab."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)

            # Create a clean empty tab via action_new_tab
            worker = app.action_new_tab()
            await worker.wait()
            await pilot.pause()

            empty_tab_id = tabs.active
            tab_count_before = tabs.tab_count

            with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
                f.write("id,name\n1,alice\n2,bob\n")
                temp_path = Path(f.name)

            try:
                await app._open_file_path(temp_path)
                await pilot.pause()
                await asyncio.sleep(0.2)
                await pilot.pause()

                # Tab count should remain the same because the empty tab was replaced
                self.assertEqual(tabs.tab_count, tab_count_before)
                self.assertEqual(tabs.active, empty_tab_id)
                self.assertEqual(app._tab_base_names.get(empty_tab_id), temp_path.name)

                pane = app.query_one(f"#{empty_tab_id}")
                editor = pane.query_one(SqlTextArea)
                self.assertIn("read_csv_auto", editor.text)
                self.assertIn(temp_path.name, editor.text)
            finally:
                temp_path.unlink(missing_ok=True)

    async def test_open_file_creates_new_tab_when_active_tab_has_query(self):
        """Opening a file when the active tab has existing SQL opens a brand new tab."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)
            initial_active_id = tabs.active

            # Ensure current tab has content
            pane1 = app.query_one(f"#{initial_active_id}")
            editor1 = pane1.query_one(SqlTextArea)
            editor1.text = "SELECT 999 AS my_custom_number;"
            await pilot.pause()

            count_before = tabs.tab_count

            with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
                f.write("col1,col2\n10,20\n30,40\n")
                csv_path = Path(f.name)

            try:
                await app._open_file_path(csv_path)
                await pilot.pause()
                await asyncio.sleep(0.2)
                await pilot.pause()

                # A new tab must have been added
                self.assertEqual(tabs.tab_count, count_before + 1)
                new_tab_id = tabs.active
                self.assertNotEqual(new_tab_id, initial_active_id)
                self.assertEqual(app._tab_base_names.get(new_tab_id), csv_path.name)

                # The original tab's SQL should still be intact
                self.assertEqual(editor1.text, "SELECT 999 AS my_custom_number;")

                # The new tab's SQL should query the CSV file
                pane2 = app.query_one(f"#{new_tab_id}")
                editor2 = pane2.query_one(SqlTextArea)
                self.assertIn("read_csv_auto", editor2.text)
                self.assertIn(csv_path.name, editor2.text)
            finally:
                csv_path.unlink(missing_ok=True)

    async def test_open_multiple_files_creates_multiple_tabs(self):
        """Opening multiple files in sequence creates distinct tabs for each file."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)

            with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f1, \
                 tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f2:
                f1.write("x,y\n1,2\n")
                f1_path = Path(f1.name)
                # Create a simple parquet file
                import duckdb
                duckdb.connect().execute(f"COPY (SELECT 1 AS a, 'test' AS b) TO '{f2.name}' (FORMAT PARQUET);")
                f2_path = Path(f2.name)

            try:
                # Open first file
                await app._open_file_path(f1_path)
                await pilot.pause()
                await asyncio.sleep(0.2)
                await pilot.pause()

                first_file_tab_id = tabs.active
                self.assertEqual(app._tab_base_names.get(first_file_tab_id), f1_path.name)
                count_after_f1 = tabs.tab_count

                # Open second file (since first tab now has query content, second must open in new tab)
                await app._open_file_path(f2_path)
                await pilot.pause()
                await asyncio.sleep(0.2)
                await pilot.pause()

                second_file_tab_id = tabs.active
                self.assertEqual(tabs.tab_count, count_after_f1 + 1)
                self.assertNotEqual(second_file_tab_id, first_file_tab_id)
                self.assertEqual(app._tab_base_names.get(second_file_tab_id), f2_path.name)
            finally:
                f1_path.unlink(missing_ok=True)
                f2_path.unlink(missing_ok=True)

    async def test_ctrl_t_shortcut_when_editor_focused(self):
        """Pressing Ctrl+T when SqlTextArea has active focus creates a new empty tab."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)
            active_pane = app.query_one(f"#{tabs.active}")
            editor = active_pane.query_one(SqlTextArea)
            editor.focus()
            await pilot.pause()
            self.assertEqual(app.focused, editor)

            initial_count = tabs.tab_count
            await pilot.press("ctrl+t")
            await asyncio.sleep(0.1)
            await pilot.pause()

            self.assertEqual(tabs.tab_count, initial_count + 1)
            new_active_id = tabs.active
            self.assertEqual(app._tab_base_names.get(new_active_id), "New Tab")
            new_pane = app.query_one(f"#{new_active_id}")
            new_editor = new_pane.query_one(SqlTextArea)
            self.assertEqual(new_editor.text.strip(), "")

    async def test_rapid_file_openings_all_execute_and_transition_to_done(self):
        """Rapidly opening multiple files creates independent tabs that all transition to done."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()
            tabs = app.query_one("#tabs", TabbedContent)

            files = []
            for i in range(4):
                f = tempfile.NamedTemporaryFile(suffix=f"_{i}.csv", mode="w", delete=False)
                f.write(f"val_{i}\n{i * 10}\n")
                f.close()
                files.append(Path(f.name))

            try:
                for p in files:
                    await app._open_file_path(p)
                    await pilot.pause()

                await asyncio.sleep(0.8)
                await pilot.pause()

                self.assertGreaterEqual(tabs.tab_count, 4)
                for pane in tabs.query("TabPane"):
                    tab_widget = tabs.get_tab(pane.id)
                    # Every tab must be done, none stuck on running
                    self.assertIn("done", tab_widget.label_text)
                    self.assertNotIn("running", tab_widget.label_text)
            finally:
                for p in files:
                    p.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
