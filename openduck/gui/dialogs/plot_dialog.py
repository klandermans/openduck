"""Modal dialog for configuring and rendering charts in the terminal using Plotly and Rich."""

import logging
from typing import TYPE_CHECKING, List, Optional, Tuple

from rich.console import Console
from rich.style import Style
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Select, Static

from openduck.core.plot_detector import (
    AVAILABLE_CHART_TYPES,
    detect_best_plot,
    render_plotly,
)

if TYPE_CHECKING:
    from openduck.gui.widgets.query_tab import QueryTab

logger = logging.getLogger(__name__)


class PlotDialog(ModalScreen):
    """Modal dialog to preview and configure Plotly charts rendered as images in the terminal."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("ctrl+r", "render_plot", "Render Plot"),
    ]

    def __init__(self, tab: "QueryTab"):
        super().__init__()
        self.tab = tab
        self._best_suggestion = None
        self._initializing = True
        self.console = Console()

    def _chart_type_options(self) -> List[Tuple[str, str]]:
        return [
            ("Bar Chart (bar)", "bar"),
            ("Line Chart (line)", "line"),
            ("Scatter Plot (scatter)", "scatter"),
            ("Histogram (hist)", "hist"),
            ("Count (count)", "count"),
            ("Pie Chart (pie)", "pie"),
            ("Box Plot (box)", "box"),
        ]

    def _col_options(self, include_none: bool = True) -> List[Tuple[str, str]]:
        opts: List[Tuple[str, str]] = []
        if include_none:
            opts.append(("(None)", "__none__"))
        for col in self.tab.column_names:
            opts.append((col, col))
        return opts

    def compose(self) -> ComposeResult:
        with Vertical(id="plot-modal"):
            yield Static("📊 Plot Results with Plotly", id="plot-dialog-title")
            with Horizontal(id="plot-controls"):
                with Vertical(classes="plot-control-col"):
                    yield Static("Chart Type:", classes="plot-label")
                    yield Select(
                        self._chart_type_options(),
                        id="select-chart-type",
                        allow_blank=False,
                        value="bar",
                    )
                with Vertical(classes="plot-control-col"):
                    yield Static("X / Category Column:", classes="plot-label")
                    yield Select(
                        self._col_options(include_none=True),
                        id="select-x-col",
                        allow_blank=False,
                        value="__none__",
                    )
                with Vertical(classes="plot-control-col"):
                    yield Static("Y / Value Column:", classes="plot-label")
                    yield Select(
                        self._col_options(include_none=True),
                        id="select-y-col",
                        allow_blank=False,
                        value="__none__",
                    )
                with Vertical(classes="plot-control-col"):
                    yield Static("Detail Mode:", classes="plot-label")
                    yield Select(
                        [
                            ("Ultra-Sharp Vector (Native Text)", "vector"),
                            ("Braille Dot Matrix (2x4 Subpixels)", "braille"),
                            ("TrueColor Blocks (Half-Block)", "halfblock"),
                        ],
                        id="select-render-mode",
                        allow_blank=False,
                        value="vector",
                    )
                with Vertical(classes="plot-control-col"):
                    yield Static("Plot Title (optional):", classes="plot-label")
                    yield Input(placeholder="Title...", id="input-plot-title")
            with Horizontal(id="plot-actions"):
                yield Button("Render Plot", variant="primary", id="btn-render-plot")
                yield Button("Close", id="btn-close")
                yield Static("", id="plot-status")
            with Vertical(id="plot-output-container"):
                yield Static(id="plot-output")

    def on_mount(self) -> None:
        """Initialize defaults and detect best chart type upon opening dialog."""
        self._initializing = True
        try:
            if not self.tab.full_data or not self.tab.column_names:
                plot_out = self.query_one("#plot-output", Static)
                plot_out.update(Text("No query data available to plot.", style=Style(color="yellow")))
                return

            self._best_suggestion = detect_best_plot(
                self.tab.column_names, self.tab.full_data
            )

            sel_type = self.query_one("#select-chart-type", Select)
            sel_x = self.query_one("#select-x-col", Select)
            sel_y = self.query_one("#select-y-col", Select)
            input_title = self.query_one("#input-plot-title", Input)

            if self._best_suggestion:
                if self._best_suggestion.chart_type in AVAILABLE_CHART_TYPES:
                    sel_type.value = self._best_suggestion.chart_type
                if self._best_suggestion.x_col and self._best_suggestion.x_col in self.tab.column_names:
                    sel_x.value = self._best_suggestion.x_col
                if self._best_suggestion.y_col and self._best_suggestion.y_col in self.tab.column_names:
                    sel_y.value = self._best_suggestion.y_col
                if self._best_suggestion.title:
                    input_title.value = self._best_suggestion.title
        finally:
            self._initializing = False

        self.action_render_plot()

    def action_render_plot(self) -> None:
        """Generate chart image using Plotly and render in terminal with Rich."""
        if not self.tab.full_data or not self.tab.column_names:
            try:
                plot_out = self.query_one("#plot-output", Static)
                plot_out.update(Text("No query data available to plot.", style=Style(color="yellow")))
            except Exception:
                pass
            return

        try:
            chart_type = self.query_one("#select-chart-type", Select).value
            x_val = self.query_one("#select-x-col", Select).value
            y_val = self.query_one("#select-y-col", Select).value
            mode_val = self.query_one("#select-render-mode", Select).value
            title = self.query_one("#input-plot-title", Input).value.strip()
            plot_out = self.query_one("#plot-output", Static)
            status = self.query_one("#plot-status", Static)
        except Exception:
            return

        x_col = None if x_val == "__none__" else x_val
        y_col = None if y_val == "__none__" else y_val
        render_mode = str(mode_val) if mode_val else "vector"

        status.update("Generating plot image...")

        try:
            container = self.query_one("#plot-output-container")
            app_size = getattr(self.app, "size", None)
            app_w = app_size.width if app_size else 100
            app_h = app_size.height if app_size else 35

            if container.size.width > 20:
                w = max(40, min(160, container.size.width - 4))
            else:
                w = max(60, min(160, app_w - 12))

            if container.size.height > 10:
                h = max(15, min(50, container.size.height - 4))
            else:
                h = max(18, min(45, app_h - 14))

            rich_output = render_plotly(
                column_names=self.tab.column_names,
                data=self.tab.full_data,
                chart_type=str(chart_type),
                x_col=str(x_col) if x_col else None,
                y_col=str(y_col) if y_col else None,
                title=title if title else None,
                width=w,
                height=h,
                mode=render_mode,
            )

            if isinstance(rich_output, Text):
                plot_out.update(rich_output)
            elif rich_output:
                plot_out.update(Text(str(rich_output), style=Style(color="yellow")))
            else:
                plot_out.update(Text("No plot output generated.", style=Style(color="yellow")))

            status.update(
                f"Rendered {chart_type} plot ({len(self.tab.full_data)} rows)"
            )
        except Exception as e:
            logger.error(f"Failed to generate Plotly chart: {e}")
            plot_out.update(Text(f"Failed to generate plot: {e}", style=Style(color="bright_red", bold=True)))
            status.update("Error generating plot")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        if event.button.id == "btn-close":
            self.dismiss()
        elif event.button.id == "btn-render-plot":
            self.action_render_plot()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Auto re-render if user changes plot parameters."""
        if getattr(self, "_initializing", False):
            return
        self.action_render_plot()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Auto re-render when Enter is pressed in title input."""
        self.action_render_plot()
