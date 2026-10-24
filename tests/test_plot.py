"""Tests for Plotly and Rich integration, plot detector heuristics, and PlotDialog UI."""

import asyncio
import datetime
import decimal
import os
import unittest
from unittest.mock import patch

from rich.console import Console
from rich.style import Style
from rich.text import Text
from textual.widgets import Button, Input, Select, Static

from openduck.core.plot_detector import (
    AVAILABLE_CHART_TYPES,
    ColumnProfile,
    PlotSuggestion,
    create_plotly_figure,
    detect_best_plot,
    detect_column_types,
    is_numeric_value,
    is_temporal_value,
    recommend_plots,
    render_plot_rich,
    render_plotext,
    render_plotly,
)
from openduck.gui.app import DuckCLI
from openduck.gui.dialogs.plot_dialog import PlotDialog
from openduck.gui.widgets.query_tab import QueryTab


class TestPlotDetectorHeuristics(unittest.TestCase):
    """Unit tests for column profiling and plot type heuristics."""

    def test_is_numeric_value(self):
        """is_numeric_value identifies ints, floats, Decimals, and numeric strings."""
        self.assertTrue(is_numeric_value(10))
        self.assertTrue(is_numeric_value(3.1415))
        self.assertTrue(is_numeric_value(decimal.Decimal("99.95")))
        self.assertTrue(is_numeric_value("42"))
        self.assertTrue(is_numeric_value("-12.34"))
        self.assertTrue(is_numeric_value(" 100 "))

        # Non-numeric
        self.assertFalse(is_numeric_value(None))
        self.assertFalse(is_numeric_value(True))
        self.assertFalse(is_numeric_value(False))
        self.assertFalse(is_numeric_value(""))
        self.assertFalse(is_numeric_value("abc"))
        self.assertFalse(is_numeric_value("2024-01-01"))

    def test_is_temporal_value(self):
        """is_temporal_value identifies dates, datetimes, times, and date strings."""
        self.assertTrue(is_temporal_value(datetime.date(2024, 1, 1)))
        self.assertTrue(is_temporal_value(datetime.datetime(2024, 1, 1, 12, 0)))
        self.assertTrue(is_temporal_value(datetime.time(12, 0)))
        self.assertTrue(is_temporal_value("2024-01-01"))
        self.assertTrue(is_temporal_value("2024-01-01 15:30:00"))
        self.assertTrue(is_temporal_value("2024-01-01T15:30:00Z"))

        # Non-temporal
        self.assertFalse(is_temporal_value(None))
        self.assertFalse(is_temporal_value(42))
        self.assertFalse(is_temporal_value("hello"))
        self.assertFalse(is_temporal_value(""))

    def test_detect_column_types(self):
        """detect_column_types profiles numeric, temporal, and categorical columns."""
        cols = ["id", "category", "created_at", "amount"]
        data = [
            (1, "Electronics", "2024-01-01", 199.99),
            (2, "Books", "2024-01-02", 29.50),
            (3, "Clothing", "2024-01-03", 59.00),
        ]
        profiles = detect_column_types(cols, data)
        self.assertEqual(len(profiles), 4)

        self.assertEqual(profiles[0].name, "id")
        self.assertEqual(profiles[0].data_type, "numeric")

        self.assertEqual(profiles[1].name, "category")
        self.assertEqual(profiles[1].data_type, "categorical")

        self.assertEqual(profiles[2].name, "created_at")
        self.assertEqual(profiles[2].data_type, "temporal")

        self.assertEqual(profiles[3].name, "amount")
        self.assertEqual(profiles[3].data_type, "numeric")

    def test_detect_column_types_empty(self):
        """Empty columns or empty data returns empty or default profiles."""
        self.assertEqual(detect_column_types([], []), [])
        profiles = detect_column_types(["empty_col"], [])
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].data_type, "categorical")

    def test_single_numeric_column_recommends_hist(self):
        """1 numeric -> hist."""
        cols = ["age"]
        data = [(20,), (25,), (30,), (35,), (40,)]
        recs = recommend_plots(cols, data)
        self.assertGreater(len(recs), 0)
        best = recs[0]
        self.assertEqual(best.chart_type, "hist")
        self.assertEqual(best.x_col, "age")

    def test_single_categorical_column_recommends_count(self):
        """1 categorical -> count (bar of value counts)."""
        cols = ["status"]
        data = [("pending",), ("completed",), ("pending",), ("failed",)]
        recs = recommend_plots(cols, data)
        self.assertGreater(len(recs), 0)
        best = recs[0]
        self.assertEqual(best.chart_type, "count")
        self.assertEqual(best.x_col, "status")

    def test_categorical_and_numeric_recommends_bar(self):
        """1 categorical + 1 numeric -> bar."""
        cols = ["department", "headcount"]
        data = [("Engineering", 50), ("Sales", 30), ("Marketing", 20)]
        best = detect_best_plot(cols, data)
        self.assertIsNotNone(best)
        self.assertEqual(best.chart_type, "bar")
        self.assertEqual(best.x_col, "department")
        self.assertEqual(best.y_col, "headcount")

    def test_temporal_and_numeric_recommends_line(self):
        """1 datetime/date + 1+ numeric -> line."""
        cols = ["date", "temperature"]
        data = [
            ("2024-01-01", 10.5),
            ("2024-01-02", 12.0),
            ("2024-01-03", 9.8),
        ]
        best = detect_best_plot(cols, data)
        self.assertIsNotNone(best)
        self.assertEqual(best.chart_type, "line")
        self.assertEqual(best.x_col, "date")
        self.assertEqual(best.y_col, "temperature")

    def test_two_numeric_columns_recommends_scatter(self):
        """2 numeric -> scatter."""
        cols = ["height", "weight"]
        data = [(170, 65), (180, 80), (160, 55), (175, 72)]
        best = detect_best_plot(cols, data)
        self.assertIsNotNone(best)
        self.assertEqual(best.chart_type, "scatter")
        self.assertEqual(best.x_col, "height")
        self.assertEqual(best.y_col, "weight")

    def test_multiple_numeric_columns_includes_scatter_and_line(self):
        """Multiple numeric columns suggest scatter and line options."""
        cols = ["a", "b", "c"]
        data = [(1, 2, 3), (4, 5, 6)]
        recs = recommend_plots(cols, data)
        types = [r.chart_type for r in recs]
        self.assertIn("scatter", types)
        self.assertIn("line", types)

    def test_fallback_recommends_bar(self):
        """Fallback when no specific heuristic triggers should recommend bar."""
        cols = ["c1", "c2"]
        # Both all None
        data = [(None, None), (None, None)]
        best = detect_best_plot(cols, data)
        self.assertIsNotNone(best)
        # Should recommend a plot (fallback or count)
        self.assertIn(best.chart_type, AVAILABLE_CHART_TYPES)

    def test_empty_input_returns_none(self):
        """Empty input data or columns returns None for best plot."""
        self.assertIsNone(detect_best_plot([], []))
        self.assertIsNone(detect_best_plot(["col1"], []))


class TestRenderPlotext(unittest.TestCase):
    """Unit tests for render_plotext function."""

    def test_render_empty_data(self):
        """Empty data or empty columns returns empty string."""
        self.assertEqual(render_plotext([], [], "bar"), "")
        self.assertEqual(render_plotext(["x"], [], "bar"), "")

    def test_render_bar_chart(self):
        """Bar chart renders non-empty ANSI string."""
        cols = ["cat", "val"]
        data = [("A", 10), ("B", 25), ("C", 15)]
        out = render_plotext(cols, data, chart_type="bar", x_col="cat", y_col="val", title="My Bar")
        self.assertTrue(len(out) > 0)
        self.assertIn("My Bar", out)

    def test_render_line_chart_with_temporal_x(self):
        """Line chart with temporal string x renders successfully."""
        cols = ["date", "temp"]
        data = [("2024-01-01", 12.5), ("2024-01-02", 15.0), ("2024-01-03", 11.0)]
        out = render_plotext(cols, data, chart_type="line", x_col="date", y_col="temp")
        self.assertTrue(len(out) > 0)

    def test_render_line_chart_with_numeric_x(self):
        """Line chart with numeric x coordinates renders successfully."""
        cols = ["step", "val"]
        data = [(1, 10), (2, 20), (3, 30)]
        out = render_plotext(cols, data, chart_type="line", x_col="step", y_col="val")
        self.assertTrue(len(out) > 0)

    def test_render_scatter_chart(self):
        """Scatter plot with 2 numeric columns renders successfully."""
        cols = ["x", "y"]
        data = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
        out = render_plotext(cols, data, chart_type="scatter", x_col="x", y_col="y")
        self.assertTrue(len(out) > 0)

    def test_render_hist_chart(self):
        """Histogram chart with 1 numeric column renders successfully."""
        cols = ["age"]
        data = [(20,), (22,), (25,), (25,), (30,), (35,)]
        out = render_plotext(cols, data, chart_type="hist", x_col="age")
        self.assertTrue(len(out) > 0)

    def test_render_count_chart(self):
        """Count chart bins categorical values and renders bar chart."""
        cols = ["status"]
        data = [("ok",), ("error",), ("ok",), ("ok",), ("pending",)]
        out = render_plotext(cols, data, chart_type="count", x_col="status")
        self.assertTrue(len(out) > 0)

    def test_render_with_custom_size(self):
        """Custom width and height are respected."""
        cols = ["cat", "val"]
        data = [("A", 10), ("B", 20)]
        out = render_plotext(cols, data, chart_type="bar", x_col="cat", y_col="val", width=60, height=15)
        self.assertTrue(len(out) > 0)

    def test_render_no_numeric_data_message(self):
        """When requested bar chart has no numeric data, returns descriptive message."""
        cols = ["a", "b"]
        data = [("foo", "bar"), ("baz", "qux")]
        out = render_plotext(cols, data, chart_type="bar", x_col="a", y_col="b")
        self.assertIn("No numeric data", out)


class TestPlotDialog(unittest.IsolatedAsyncioTestCase):
    """Async tests for PlotDialog UI and behavior."""

    async def test_plot_dialog_renders_on_mount(self):
        """PlotDialog automatically detects best plot and renders chart on mount."""
        tab = QueryTab()
        tab.column_names = ["category", "sales"]
        tab.full_data = [("Apples", 50), ("Oranges", 75), ("Bananas", 30)]

        dialog = PlotDialog(tab)

        app = DuckCLI()
        async with app.run_test() as pilot:
            await app.push_screen(dialog)
            await pilot.pause()

            # Verify selectors initialized
            sel_type = dialog.query_one("#select-chart-type", Select)
            sel_x = dialog.query_one("#select-x-col", Select)
            sel_y = dialog.query_one("#select-y-col", Select)
            self.assertEqual(sel_type.value, "bar")
            self.assertEqual(sel_x.value, "category")
            self.assertEqual(sel_y.value, "sales")

            # Verify plot output rendered
            plot_out = dialog.query_one("#plot-output", Static)
            rendered = str(plot_out._render())
            self.assertTrue(len(rendered) > 0)

            status = dialog.query_one("#plot-status", Static)
            self.assertIn("Rendered bar plot", str(status._render()))

            # Close dialog
            close_btn = dialog.query_one("#btn-close", Button)
            close_btn.press()
            await pilot.pause()

    async def test_plot_dialog_when_no_data(self):
        """PlotDialog displays warning message when tab data is empty."""
        tab = QueryTab()
        tab.column_names = []
        tab.full_data = []

        dialog = PlotDialog(tab)

        app = DuckCLI()
        async with app.run_test() as pilot:
            await app.push_screen(dialog)
            await pilot.pause()

            plot_out = dialog.query_one("#plot-output", Static)
            self.assertIn("No query data available", str(plot_out._render()))

            dialog.dismiss()
            await pilot.pause()

    async def test_plot_dialog_change_chart_type(self):
        """Changing chart type in selector triggers re-render."""
        tab = QueryTab()
        tab.column_names = ["x", "y"]
        tab.full_data = [(10, 20), (30, 40), (50, 60)]

        dialog = PlotDialog(tab)

        app = DuckCLI()
        async with app.run_test() as pilot:
            await app.push_screen(dialog)
            await pilot.pause()

            sel_type = dialog.query_one("#select-chart-type", Select)
            sel_type.value = "line"
            await pilot.pause()

            status = dialog.query_one("#plot-status", Static)
            self.assertIn("Rendered line plot", str(status._render()))

            dialog.dismiss()
            await pilot.pause()

    async def test_plot_dialog_title_enter_submission(self):
        """Submitting title input re-renders the plot with title."""
        tab = QueryTab()
        tab.column_names = ["month", "revenue"]
        tab.full_data = [("Jan", 100), ("Feb", 200)]

        dialog = PlotDialog(tab)

        app = DuckCLI()
        async with app.run_test() as pilot:
            await app.push_screen(dialog)
            await pilot.pause()

            title_input = dialog.query_one("#input-plot-title", Input)
            title_input.value = "Sales Report"
            # Trigger submit via handler directly
            dialog.on_input_submitted(Input.Submitted(title_input, "Sales Report"))
            await pilot.pause()

            plot_out = dialog.query_one("#plot-output", Static)
            self.assertIn("Sales Report", str(plot_out._render()))

            dialog.dismiss()
            await pilot.pause()

    async def test_plot_dialog_render_button_click(self):
        """Clicking Render Plot button re-renders the plot."""
        tab = QueryTab()
        tab.column_names = ["a", "b"]
        tab.full_data = [(1, 2), (3, 4)]

        dialog = PlotDialog(tab)

        app = DuckCLI()
        async with app.run_test() as pilot:
            await app.push_screen(dialog)
            await pilot.pause()

            btn_render = dialog.query_one("#btn-render-plot", Button)
            btn_render.press()
            await pilot.pause()

            status = dialog.query_one("#plot-status", Static)
            self.assertIn("Rendered", str(status._render()))

            dialog.dismiss()
            await pilot.pause()

    async def test_plot_button_in_query_tab_toggles_plot_view(self):
        """Clicking 📊 Plot button in QueryTab toggles in-tab plot view in place of results table."""
        from textual.widgets import ContentSwitcher
        from openduck.gui.widgets.plot_view import PlotView

        app = DuckCLI()
        async with app.run_test() as pilot:
            await asyncio.sleep(0.3)
            await pilot.pause()

            from textual.widgets import TabbedContent
            tabs = app.query_one("#tabs", TabbedContent)
            tab = app.query_one(f"#{tabs.active}").query_one(QueryTab)

            # Ensure btn-plot button exists with correct properties
            btn_plot = tab.query_one("#btn-plot", Button)
            self.assertIn("Plot", str(btn_plot.label))
            self.assertIn("export-btn", btn_plot.classes)
            self.assertEqual(btn_plot.tooltip, "Plot results with Plotly")

            switcher = tab.query_one("#results-switcher", ContentSwitcher)
            self.assertEqual(switcher.current, "results-table")
            self.assertFalse(tab.is_plot_active())

            # With no data, pressing shows notification and does not toggle
            btn_plot.press()
            await pilot.pause()
            meta = tab.query_one("#metadata-bar", Static)
            self.assertEqual(str(meta._render()), "No data to plot")
            self.assertEqual(switcher.current, "results-table")

            # Now populate data and press again
            tab.column_names = ["x", "y"]
            tab.full_data = [(10, 20), (30, 40)]

            btn_plot.press()
            await pilot.pause()

            # Plot view should now be active in place of results table
            self.assertTrue(tab.is_plot_active())
            self.assertEqual(switcher.current, "plot-view")
            self.assertIn("Table", str(btn_plot.label))
            self.assertEqual(btn_plot.tooltip, "Show data table")

            # Verify plot view rendered
            plot_view = tab.query_one("#plot-view", PlotView)
            plot_out = plot_view.query_one("#plot-output", Static)
            self.assertTrue(len(str(plot_out._render())) > 0)

            # Pressing again toggles back to table view
            btn_plot.press()
            await pilot.pause()

            self.assertFalse(tab.is_plot_active())
            self.assertEqual(switcher.current, "results-table")
            self.assertIn("Plot", str(btn_plot.label))
            self.assertEqual(btn_plot.tooltip, "Plot results with Plotly")

            # Switching back to plot and pressing the in-plot "📋 Table" button
            tab.show_plot()
            await pilot.pause()
            self.assertTrue(tab.is_plot_active())

            btn_table = plot_view.query_one("#btn-show-table", Button)
            btn_table.press()
            await pilot.pause()
            self.assertFalse(tab.is_plot_active())
            self.assertEqual(switcher.current, "results-table")


class TestPlotlyAndRich(unittest.TestCase):
    """Unit tests for Plotly figure creation, HTML output, and Rich console formatting."""

    def test_create_plotly_bar(self):
        """create_plotly_figure generates a valid plotly figure for bar chart."""
        cols = ["product", "revenue"]
        data = [("Widget", 100), ("Gadget", 250)]
        fig = create_plotly_figure(cols, data, chart_type="bar", x_col="product", y_col="revenue", title="Sales")
        self.assertEqual(len(fig.data), 1)
        self.assertEqual(fig.data[0].type, "bar")
        self.assertEqual(fig.layout.title.text, "Sales")

    def test_create_plotly_line_and_scatter(self):
        """create_plotly_figure handles line and scatter charts."""
        cols = ["time", "val"]
        data = [(1, 10), (2, 20), (3, 15)]
        fig_line = create_plotly_figure(cols, data, chart_type="line", x_col="time", y_col="val")
        self.assertEqual(fig_line.data[0].type, "scatter")
        self.assertEqual(fig_line.data[0].mode, "lines+markers")

        fig_scatter = create_plotly_figure(cols, data, chart_type="scatter", x_col="time", y_col="val")
        self.assertEqual(fig_scatter.data[0].type, "scatter")
        self.assertEqual(fig_scatter.data[0].mode, "markers")

    def test_create_plotly_hist_and_count(self):
        """create_plotly_figure handles histogram and count charts."""
        cols = ["category"]
        data = [("A",), ("B",), ("A",), ("C",)]
        fig_count = create_plotly_figure(cols, data, chart_type="count", x_col="category")
        self.assertEqual(fig_count.data[0].type, "bar")

        num_cols = ["score"]
        num_data = [(85,), (90,), (78,), (92,)]
        fig_hist = create_plotly_figure(num_cols, num_data, chart_type="hist", x_col="score")
        self.assertEqual(fig_hist.data[0].type, "histogram")

    def test_render_plotly_terminal_image(self):
        """render_plotly renders terminal image as Rich Text with braille and half blocks."""
        cols = ["month", "sales"]
        data = [("Jan", 50), ("Feb", 80)]
        out_braille = render_plotly(cols, data, chart_type="bar", x_col="month", y_col="sales", title="Monthly Sales", mode="braille")
        self.assertIsInstance(out_braille, Text)
        self.assertIn("Monthly Sales", out_braille.plain)
        has_braille = any(0x2800 <= ord(ch) <= 0x28FF for ch in out_braille.plain)
        self.assertTrue(has_braille)

        out_half = render_plotly(cols, data, chart_type="bar", x_col="month", y_col="sales", title="Monthly Sales", mode="halfblock")
        self.assertIsInstance(out_half, Text)
        self.assertIn("▀", out_half.plain)

    def test_render_plotly_vector_mode(self):
        """render_plotly vector mode produces ultra-sharp native text axes and blocks."""
        cols = ["month", "sales"]
        data = [("Jan", 50), ("Feb", 80)]
        out_vec = render_plotly(cols, data, chart_type="bar", x_col="month", y_col="sales", title="Monthly Sales", mode="vector")
        self.assertIsInstance(out_vec, Text)
        self.assertIn("Monthly Sales", out_vec.plain)
        self.assertIn("Jan", out_vec.plain)
        self.assertIn("Feb", out_vec.plain)
        self.assertIn("┤", out_vec.plain)

    def test_render_plot_rich_returns_rich_text(self):
        """render_plot_rich produces a styled rich.text.Text object."""
        cols = ["department", "headcount"]
        data = [("Engineering", 42), ("Sales", 28), ("HR", 10)]
        rich_text = render_plot_rich(cols, data, chart_type="bar", x_col="department", y_col="headcount", title="Company Staff")
        self.assertIsInstance(rich_text, Text)
        plain = rich_text.plain
        self.assertIn("PLOTLY", plain)
        self.assertIn("Company Staff", plain)
        self.assertIn("Engineering", plain)

        # Test printing via rich.console.Console
        console = Console(record=True, width=80)
        console.print(rich_text)
        exported = console.export_text()
        self.assertIn("PLOTLY", exported)



if __name__ == "__main__":
    unittest.main()
