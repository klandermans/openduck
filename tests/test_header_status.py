"""Tests for terminal tab status indicators and in-app header styling."""

import asyncio
import io
import unittest
from unittest.mock import MagicMock, patch

from textual.widgets import Header

from openduck.gui.app import DuckCLI


class TestHeaderStatus(unittest.TestCase):
    """Test the terminal tab, header formatting and window status updates."""

    def test_format_title_done(self):
        """format_title should style title bold with green dot and green subtitle on line 2."""
        app = DuckCLI()
        content = app.format_title("OpenDuck", "done")
        self.assertEqual(content.plain, "OpenDuck •\ndone")
        # Check spans: bold on title, green on dot, green on subtitle
        spans = list(content.spans)
        self.assertEqual(spans[0].style, "bold")
        self.assertEqual(spans[1].style, "green")
        self.assertEqual(spans[2].style, "green")

        # Also support legacy string with dot prefix
        content_legacy = app.format_title("OpenDuck", "• done")
        self.assertEqual(content_legacy.plain, "OpenDuck •\ndone")
        content_legacy2 = app.format_title("OpenDuck", "● done")
        self.assertEqual(content_legacy2.plain, "OpenDuck •\ndone")

    def test_format_title_running(self):
        """format_title should style red dot and red subtitle when running."""
        app = DuckCLI()
        content = app.format_title("OpenDuck", "running")
        self.assertEqual(content.plain, "OpenDuck •\nrunning")
        spans = list(content.spans)
        self.assertEqual(spans[0].style, "bold")
        self.assertEqual(spans[1].style, "red")
        self.assertEqual(spans[2].style, "red")

    def test_format_title_error(self):
        """format_title should style red dot and red subtitle on error."""
        app = DuckCLI()
        content = app.format_title("OpenDuck", "error")
        self.assertEqual(content.plain, "OpenDuck •\nerror")
        spans = list(content.spans)
        self.assertEqual(spans[0].style, "bold")
        self.assertEqual(spans[1].style, "red")
        self.assertEqual(spans[2].style, "red")

    def test_format_title_cancelled(self):
        """format_title should style yellow dot and yellow subtitle on cancelled."""
        app = DuckCLI()
        content = app.format_title("OpenDuck", "cancelled")
        self.assertEqual(content.plain, "OpenDuck •\ncancelled")
        spans = list(content.spans)
        self.assertEqual(spans[0].style, "bold")
        self.assertEqual(spans[1].style, "yellow")
        self.assertEqual(spans[2].style, "yellow")

    def test_format_title_with_details(self):
        """format_title with details should keep details in subtitle on line 2."""
        app = DuckCLI()
        content = app.format_title("OpenDuck", "done (0.04s)")
        self.assertEqual(content.plain, "OpenDuck •\ndone (0.04s)")
        spans = list(content.spans)
        self.assertEqual(spans[0].style, "bold")
        self.assertEqual(spans[1].style, "green")
        self.assertEqual(spans[2].style, "green")

    def test_default_title_and_subtitle(self):
        """DuckCLI should define title OpenDuck and initial status done."""
        self.assertEqual(DuckCLI.TITLE, "OpenDuck")
        self.assertEqual(DuckCLI.SUB_TITLE, "done")

    def test_set_status_running(self):
        """Setting status to running should update subtitle and format terminal title."""
        app = DuckCLI()
        written = []

        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("running")
        self.assertEqual(app.sub_title, "running")
        self.assertTrue(any("OpenDuck • running" in s for s in written))
        self.assertTrue(any("\033]0;" in s for s in written))
        self.assertTrue(any("\033]1;" in s for s in written))
        self.assertTrue(any("\033]2;" in s for s in written))

    def test_set_status_done(self):
        """Setting status to done should update subtitle and format terminal title."""
        app = DuckCLI()
        written = []

        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("done")
        self.assertEqual(app.sub_title, "done")
        self.assertTrue(any("OpenDuck • done" in s for s in written))

    def test_set_status_ready(self):
        """Setting status to ready should format as OpenDuck • ready."""
        app = DuckCLI()
        written = []

        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("ready")
        self.assertEqual(app.sub_title, "ready")
        self.assertTrue(any("OpenDuck • ready" in s for s in written))

    def test_set_status_error_and_cancelled(self):
        """Setting status to error and cancelled should update subtitle and terminal title."""
        app = DuckCLI()
        written = []

        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("error")
        self.assertEqual(app.sub_title, "error")
        self.assertTrue(any("OpenDuck • error" in s for s in written))

        app.set_status("cancelled")
        self.assertEqual(app.sub_title, "cancelled")
        self.assertTrue(any("OpenDuck • cancelled" in s for s in written))

    def test_set_status_with_details(self):
        """Details should be included in subtitle and terminal title when provided."""
        app = DuckCLI()
        written = []

        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("done", details="0.04s")
        self.assertEqual(app.sub_title, "done (0.04s)")
        self.assertTrue(any("OpenDuck • done (0.04s)" in s for s in written))

    def test_set_status_custom_icon(self):
        """Explicit icon override should take precedence in terminal title."""
        app = DuckCLI()
        written = []
        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app.set_status("custom", icon="⭐")
        self.assertEqual(app.sub_title, "custom")
        self.assertTrue(any("OpenDuck ⭐ custom" in s for s in written))

    def test_terminal_title_stack(self):
        """Push and pop should write standard ANSI window title stack sequences."""
        app = DuckCLI()
        written = []
        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        app._push_terminal_title()
        self.assertIn("\033[22;0t", written)

        app._pop_terminal_title()
        self.assertIn("\033[23;0t", written)

    def test_fallback_stdout_when_driver_is_none(self):
        """If driver is None, sys.__stdout__ is used if it is a tty."""
        app = DuckCLI()
        app._driver = None

        mock_stdout = MagicMock()
        mock_stdout.isatty.return_value = True

        with patch("sys.__stdout__", mock_stdout):
            app._set_terminal_title("Test Title")
            mock_stdout.write.assert_called_once()
            self.assertIn("Test Title", mock_stdout.write.call_args[0][0])
            mock_stdout.flush.assert_called_once()

    def test_set_status_edge_cases(self):
        """set_status handles case insensitivity, prefix stripping, and custom labels."""
        app = DuckCLI()
        written = []
        mock_driver = MagicMock()
        mock_driver.write = written.append
        app._driver = mock_driver

        # Prefix stripping
        app.set_status("• running")
        self.assertTrue(any("OpenDuck • running" in s for s in written))

        written.clear()
        app.set_status("● running")
        self.assertTrue(any("OpenDuck • running" in s for s in written))

        written.clear()
        app.set_status("🟢 done")
        self.assertTrue(any("OpenDuck • done" in s for s in written))

        # Uppercase
        written.clear()
        app.set_status("RUNNING")
        self.assertTrue(any("OpenDuck • RUNNING" in s for s in written))

        # Fail keyword
        written.clear()
        app.set_status("failed")
        self.assertTrue(any("OpenDuck • failed" in s for s in written))

        # Multiple prefixes
        written.clear()
        app.set_status("● 🟢 done")
        self.assertTrue(any("OpenDuck • done" in s for s in written))

        # Executing status
        written.clear()
        app.set_status("executing")
        self.assertTrue(any("OpenDuck • executing" in s for s in written))

        # Empty string
        written.clear()
        app.set_status("")
        self.assertTrue(any("OpenDuck •" in s for s in written))

    def test_format_title_empty_or_none(self):
        """format_title should handle empty and None gracefully without crashing."""
        app = DuckCLI()
        # None arguments fall back to defaults
        c1 = app.format_title(None, None)
        self.assertEqual(c1.plain, "OpenDuck •\ndone")

        # Empty title falls back to DuckCLI.TITLE
        c2 = app.format_title("", "done")
        self.assertEqual(c2.plain, "OpenDuck •\ndone")

        # Empty or whitespace subtitle produces only line 1
        c3 = app.format_title("OpenDuck", "")
        self.assertEqual(c3.plain, "OpenDuck •")
        c4 = app.format_title("OpenDuck", "   ")
        self.assertEqual(c4.plain, "OpenDuck •")

    def test_format_title_ready_and_fail(self):
        """format_title should style ready as green and fail as red."""
        app = DuckCLI()
        c_ready = app.format_title("OpenDuck", "ready")
        self.assertEqual(c_ready.plain, "OpenDuck •\nready")
        self.assertEqual(list(c_ready.spans)[1].style, "green")
        self.assertEqual(list(c_ready.spans)[2].style, "green")

        c_fail = app.format_title("OpenDuck", "failed")
        self.assertEqual(c_fail.plain, "OpenDuck •\nfailed")
        self.assertEqual(list(c_fail.spans)[1].style, "red")
        self.assertEqual(list(c_fail.spans)[2].style, "red")


class TestHeaderStatusAsync(unittest.IsolatedAsyncioTestCase):
    """Async tests with running Textual app pilot."""

    async def test_header_widget_and_status_updates(self):
        """Header widget must be present in window, and status updates subtitle/tab."""
        app = DuckCLI()
        titles = []
        app._set_terminal_title = titles.append

        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            # Ensure Header is present in window with show_clock=False and height 2
            headers = list(app.query(Header))
            self.assertEqual(len(headers), 1)
            self.assertFalse(headers[0]._show_clock)
            self.assertEqual(headers[0].styles.height.value, 2)

            # Initially done
            self.assertEqual(app.sub_title, "done")
            self.assertIn("OpenDuck • done", titles)

            # Check in-app header content
            title_widget = headers[0].query_one("HeaderTitle")
            self.assertEqual(title_widget._render().plain, "OpenDuck •\ndone")

            # Update to running
            app.set_status("running")
            await pilot.pause()
            self.assertEqual(app.sub_title, "running")
            self.assertIn("OpenDuck • running", titles)
            self.assertEqual(title_widget._render().plain, "OpenDuck •\nrunning")

            # Update to cancelled
            app.set_status("cancelled")
            await pilot.pause()
            self.assertEqual(app.sub_title, "cancelled")
            self.assertIn("OpenDuck • cancelled", titles)
            self.assertEqual(title_widget._render().plain, "OpenDuck •\ncancelled")

            # Update to error
            app.set_status("error")
            await pilot.pause()
            self.assertEqual(app.sub_title, "error")
            self.assertIn("OpenDuck • error", titles)
            self.assertEqual(title_widget._render().plain, "OpenDuck •\nerror")

            # Update back to done
            app.set_status("done")
            await pilot.pause()
            self.assertEqual(app.sub_title, "done")
            self.assertEqual(titles[-1], "OpenDuck • done")
            self.assertEqual(title_widget._render().plain, "OpenDuck •\ndone")

    async def test_action_run_query_status_lifecycle(self):
        """Query execution lifecycle sets running and then done on completion."""
        app = DuckCLI()
        status_history = []

        orig_set_status = app.set_status

        def tracking_set_status(status, icon=None, details=None):
            status_history.append(status)
            return orig_set_status(status, icon=icon, details=details)

        app.set_status = tracking_set_status

        async with app.run_test() as pilot:
            # Wait for initial tab to be added (timer is 0.2s)
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab
            from openduck.gui.widgets.sql_editor import SqlTextArea

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.text = "SELECT 42 AS result;"

            # Clear initial mount statuses
            status_history.clear()

            # Run query
            await app.action_run_query()
            await pilot.pause()

            self.assertIn("running", status_history)
            self.assertEqual(status_history[-1], "done")
            self.assertEqual(app.sub_title, "done")

    async def test_action_run_query_error_status(self):
        """Query with invalid SQL sets status to error."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            # Wait for initial tab to be added (timer is 0.2s)
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab
            from openduck.gui.widgets.sql_editor import SqlTextArea

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.text = "SELECT * FROM non_existent_table_xyz_123;"

            await app.action_run_query()
            await pilot.pause()

            self.assertEqual(app.sub_title, "error")

    async def test_close_tab_with_running_query_resets_status(self):
        """Closing a tab with a running query cancels the task and resets status to done."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab

            tabs = app.query_one("#tabs", TabbedContent)
            tab_id = tabs.active
            tab = app.query_one(f"#{tab_id}").query_one(QueryTab)

            # Simulate a running task
            fake_task = asyncio.create_task(asyncio.sleep(10))
            tab.running_task = fake_task
            app.set_status("running")
            self.assertEqual(app.sub_title, "running")

            # Close tab
            app._close_tab_by_id(tab_id)
            await pilot.pause()

            self.assertTrue(fake_task.cancelled() or fake_task.done())
            self.assertEqual(app.sub_title, "done")

    async def test_set_status_during_modal_screen(self):
        """Status updates correctly even when a modal screen is active."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.screen import ModalScreen
            from textual.widgets import Label

            class DummyModal(ModalScreen):
                def compose(self):
                    yield Label("Modal content")

            modal = DummyModal()
            await app.push_screen(modal)
            await pilot.pause()

            # Update status while modal is the active screen
            app.set_status("running")
            await pilot.pause()
            self.assertEqual(app.sub_title, "running")

            await app.pop_screen()
            await pilot.pause()

            app.set_status("done")
            await pilot.pause()
            self.assertEqual(app.sub_title, "done")

    async def test_multitab_concurrent_status(self):
        """When multiple tabs have running queries, finishing one leaves status as running until all finish."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab

            # Create second tab
            await app.add_new_tab("Tab 2", "SELECT 1;", run=False)
            await pilot.pause()

            # Find the two QueryTabs
            qtabs = list(app.query(QueryTab).results())
            self.assertGreaterEqual(len(qtabs), 2)

            qtab1, qtab2 = qtabs[0], qtabs[1]

            # Simulate both running
            task1 = asyncio.create_task(asyncio.sleep(10))
            task2 = asyncio.create_task(asyncio.sleep(10))
            qtab1.running_task = task1
            qtab2.running_task = task2

            app.set_status("running")
            self.assertEqual(app.sub_title, "running")

            # Finish task 1
            task1.cancel()
            qtab1.running_task = None

            # Check: since task2 is still running, is_any_query_running is True
            self.assertTrue(app.is_any_query_running())

            # Finish task 2
            task2.cancel()
            qtab2.running_task = None

            self.assertFalse(app.is_any_query_running())
            app.set_status("done")
            self.assertEqual(app.sub_title, "done")

    async def test_cancel_query_button_sets_cancelled_status(self):
        """Clicking the cancel button on a running query updates status to cancelled."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent, Button
            from openduck.gui.widgets.query_tab import QueryTab
            from openduck.gui.widgets.sql_editor import SqlTextArea

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.text = "SELECT 1;"

            async def slow_to_thread(fn):
                await asyncio.sleep(5)
                return fn()

            with patch("openduck.gui.app.asyncio.to_thread", side_effect=slow_to_thread):
                app.run_worker(app.action_run_query())
                await asyncio.sleep(0.05)
                self.assertEqual(app.sub_title, "running")

                cancel_btn = tab.query_one("#cancel-query", Button)
                cancel_btn.press()
                await pilot.pause()
                await asyncio.sleep(0.05)

                self.assertEqual(app.sub_title, "cancelled")

                # Cancel remaining workers
                for w in app.workers:
                    w.cancel()
                await asyncio.sleep(0.05)

    async def test_duckdb_interrupt_exception_handled_as_cancelled(self):
        """DuckDB InterruptException should set status to cancelled rather than error."""
        import duckdb
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab
            from openduck.gui.widgets.sql_editor import SqlTextArea

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.text = "SELECT 1;"

            async def interrupt_to_thread(fn):
                raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

            with patch("openduck.gui.app.asyncio.to_thread", side_effect=interrupt_to_thread):
                await app.action_run_query()
                await pilot.pause()

                self.assertEqual(app.sub_title, "cancelled")

    async def test_multiquery_on_same_tab_does_not_clobber_executing_meta(self):
        """Starting a new query cancels older query without older query clobbering Executing... meta."""
        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            from openduck.gui.widgets.query_tab import QueryTab
            from openduck.gui.widgets.sql_editor import SqlTextArea

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.text = "SELECT 1;"

            task1_event = asyncio.Event()

            async def slow_to_thread1(fn):
                await task1_event.wait()
                return fn()

            with patch("openduck.gui.app.asyncio.to_thread", side_effect=slow_to_thread1):
                app.run_worker(app.action_run_query())
                await asyncio.sleep(0.05)
                self.assertEqual(app.sub_title, "running")

                # Launch second query on same tab
                task2_run = asyncio.Event()

                async def slow_to_thread2(fn):
                    task2_run.set()
                    await asyncio.sleep(0.1)
                    return [[1]], ["col"], None, 0.05, False

                with patch("openduck.gui.app.asyncio.to_thread", side_effect=slow_to_thread2):
                    app.run_worker(app.action_run_query())
                    await asyncio.sleep(0.05)

                    # Release task 1 so it handles cancellation
                    task1_event.set()
                    await asyncio.sleep(0.1)
                    await pilot.pause()

                    # Query 2 should still be tracked or completed as done
                    self.assertEqual(app.sub_title, "done")

    async def test_export_status_preserves_running_when_queries_active(self):
        """When an export completes while other queries are running, status remains running."""
        from openduck.gui.dialogs.export_dialog import ExportDialog
        from textual.widgets import TabbedContent
        from openduck.gui.widgets.query_tab import QueryTab

        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)
            tab.full_data = [["val1"]]
            tab.column_names = ["c1"]

            # Mark a simulated running query on the tab
            fake_task = asyncio.create_task(asyncio.sleep(10))
            tab.running_task = fake_task
            app.set_status("running")

            dialog = ExportDialog(tab, "csv")
            with patch.object(dialog, "export_to_csv", return_value=(True, "")):
                await app.push_screen(dialog)
                await pilot.pause()

                export_btn = dialog.query_one("#btn-export")
                export_btn.press()
                await pilot.pause()
                await asyncio.sleep(0.05)

                # Status should still be running because fake_task is still running
                self.assertEqual(app.sub_title, "running")

            fake_task.cancel()


if __name__ == "__main__":
    unittest.main()

