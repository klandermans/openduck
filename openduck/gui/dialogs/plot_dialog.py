"""Modal dialog for configuring and rendering terminal charts using plotext."""

import logging
from typing import TYPE_CHECKING, List, Optional, Tuple

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Select, Static

from openduck.core.plot_detector import (
    AVAILABLE_CHART_TYPES,
    detect_best_plot,
    render_plotext,
)

if TYPE_CHECKING:
    from openduck.gui.widgets.query_tab import QueryTab

logger = logging.getLogger(__name__)


class PlotDialog(ModalScreen):
    """Modal dialog to preview and configure plotext charts."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("ctrl+r", "render_plot", "Render Plot"),
    ]

    def __init__(self, tab: "QueryTab"):
        super().__init__()
        self.tab = tab
        self._best_suggestion = None
        self._initializing = True

    def _chart_type_options(self) -> List[Tuple[str, str]]:
        return [
            ("Bar Chart (bar)", "bar"),
            ("Line Chart (line)", "line"),
            ("Scatter Plot (scatter)", "scatter"),
            ("Histogram (hist)", "hist"),
            ("Count (count)", "count"),
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
            yield Static("📊 Plot Results with plotext", id="plot-dialog-title")
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
                plot_out.update(Text("No query data available to plot.", style="yellow"))
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
        """Generate chart using plotext and update ANSI display."""
        if not self.tab.full_data or not self.tab.column_names:
            try:
                plot_out = self.query_one("#plot-output", Static)
                plot_out.update(Text("No query data available to plot.", style="yellow"))
            except Exception:
                pass
            return

        try:
            chart_type = self.query_one("#select-chart-type", Select).value
            x_val = self.query_one("#select-x-col", Select).value
            y_val = self.query_one("#select-y-col", Select).value
            title = self.query_one("#input-plot-title", Input).value.strip()
            plot_out = self.query_one("#plot-output", Static)
            status = self.query_one("#plot-status", Static)
        except Exception:
            return

        x_col = None if x_val == "__none__" else x_val
        y_col = None if y_val == "__none__" else y_val

        status.update("Generating plot...")

        try:
            container = self.query_one("#plot-output-container")
            w = max(40, container.size.width - 4) if container.size.width > 20 else 80
            h = max(10, container.size.height - 4) if container.size.height > 10 else 22

            ansi_plot = render_plotext(
                column_names=self.tab.column_names,
                data=self.tab.full_data,
                chart_type=str(chart_type),
                x_col=str(x_col) if x_col else None,
                y_col=str(y_col) if y_col else None,
                title=title if title else None,
                width=w,
                height=h,
            )

            if ansi_plot:
                plot_out.update(Text.from_ansi(ansi_plot))
                status.update(
                    f"Rendered {chart_type} plot ({len(self.tab.full_data)} rows)"
                )
            else:
                plot_out.update(Text("No plot output generated.", style="yellow"))
                status.update("No plot output")
        except Exception as e:
            logger.error(f"Failed to generate plotext chart: {e}")
            plot_out.update(Text(f"Failed to generate plot: {e}", style="bold red"))
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
