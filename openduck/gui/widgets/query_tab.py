"""Query tab container combining editor, results table, action buttons, and status bar."""

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Static

from openduck.gui.widgets.results_table import ResultsTable
from openduck.gui.widgets.sql_editor import SqlTextArea


class QueryTab(Vertical):
    """A tab containing a SQL text editor, results table, action toolbar, and metadata bar."""

    def __init__(self, sql: str = "", connection_id: str = None):
        super().__init__()
        self.initial_sql = sql
        self.full_data, self.column_names, self.col_states = [], [], {}
        self.running_task = None
        self.connection_id = connection_id

    def compose(self) -> ComposeResult:
        text_area = SqlTextArea(
            self.initial_sql,
            language="sql",
            id="query-input",
            theme="vscode_dark",
            soft_wrap=False,  # Disabled to prevent crash with long lines
            read_only=False,
            tab_behavior="indent",
            show_line_numbers=True,
        )
        # Force SQL highlighting
        try:
            text_area.language = "sql"
        except Exception:
            pass
        yield text_area
        with Horizontal():  # Container for table and export sidebar
            yield ResultsTable(id="results-table")
            with Vertical(id="export-sidebar"):
                yield Button("▶ Run", id="btn-run-query", classes="export-btn", variant="primary", tooltip="Run query / selection (Ctrl+Enter)")
                yield Button("💬 --", id="btn-comment", classes="export-btn", tooltip="Toggle comment (F3 / Cmd+/)")
                yield Button("↓CSV", id="export-csv", classes="export-btn")
                yield Button("↓XLSX", id="export-excel", classes="export-btn")
                yield Button("Abort", id="cancel-query", classes="export-btn", tooltip="Cancel Query", variant="error")
        with Horizontal(id="metadata-container"):
            yield Static("Ready", id="metadata-bar")
            yield Static("Memory: N/A", id="memory-bar")

    def on_mount(self):
        t = self.query_one(DataTable)
        t.cursor_type, t.zebra_stripes = "row", True
        # Use slight delay for focus to allow UI to settle
        self.set_timer(0.1, self.safe_focus)

    def safe_focus(self):
        """Focus the editor safely."""
        try:
            editor = self.query_one("#query-input")
            editor.move_cursor((0, 0))
            editor.focus()
        except Exception:
            pass
