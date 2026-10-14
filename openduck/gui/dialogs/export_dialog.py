"""Modal dialog for exporting query results to CSV or Excel."""

import asyncio
import csv
import logging
import re
from typing import TYPE_CHECKING, Tuple
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static, TextArea

from openduck.core.config import CWD

if TYPE_CHECKING:
    from openduck.gui.widgets.query_tab import QueryTab

logger = logging.getLogger(__name__)


class ExportDialog(ModalScreen):
    """Modal dialog to export query results into CSV or XLSX files."""

    def __init__(self, tab: 'QueryTab', export_type: str):
        super().__init__()
        self.tab = tab
        self.export_type = export_type

    def compose(self) -> ComposeResult:
        with Vertical(id="export-modal"):
            yield Static(f"Export {self.export_type.upper()}", id="export-modal-title")
            yield Input(placeholder="Enter filename...", id="export-filename")
            yield Static("Current query:", id="current-query-label")
            yield Static(
                self.tab.query_one(TextArea).text[:100]
                + ("..." if len(self.tab.query_one(TextArea).text) > 100 else ""),
                id="current-query-preview",
            )
            with Horizontal():
                yield Button("Export", variant="primary", id="btn-export")
                yield Button("Cancel", id="btn-cancel")

    def on_mount(self):
        # Pre-populate filename based on query content
        query_text = self.tab.query_one(TextArea).text.strip()
        table_match = re.search(r'FROM\s+(\w+)', query_text, re.IGNORECASE)
        base_name = table_match.group(1) if table_match else "query_result"
        base_name = re.sub(r'[^\w\-_]', '_', base_name)
        extension = ".csv" if self.export_type == "csv" else ".xlsx"
        default_filename = f"{base_name}{extension}"

        self.query_one("#export-filename").value = default_filename
        self.query_one("#export-filename").focus()

    def export_to_csv(self, filename: str) -> Tuple[bool, str]:
        """Export table data to CSV file."""
        try:
            filepath = CWD / filename
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.tab.column_names)
                for row in self.tab.full_data:
                    writer.writerow(row)
            return True, ""
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return False, str(e)

    def export_to_excel(self, filename: str) -> Tuple[bool, str]:
        """Export table data to Excel file."""
        try:
            import pandas as pd
        except ImportError:
            return False, "pandas is not installed (pip install pandas openpyxl)"
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            return False, "openpyxl is not installed (pip install openpyxl)"
        try:
            filepath = CWD / filename
            df = pd.DataFrame(self.tab.full_data, columns=self.tab.column_names)
            df.to_excel(filepath, index=False)
            return True, ""
        except Exception as e:
            logger.error(f"Failed to export Excel: {e}")
            return False, str(e)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-cancel":
            self.dismiss()
        elif event.button.id == "btn-export":
            filename = self.query_one("#export-filename", Input).value.strip()
            if not filename:
                self.query_one("#export-filename").value = ""
                self.query_one("#export-filename").focus()
                return

            event.button.disabled = True
            if self.export_type == "csv":
                success, error_msg = await asyncio.to_thread(self.export_to_csv, filename)
            else:
                success, error_msg = await asyncio.to_thread(self.export_to_excel, filename)

            event.button.disabled = False
            if success:
                self.app.notify(f"Geëxporteerd naar {filename}", timeout=2.0)
                self.dismiss({"exported": True, "filename": filename})
            else:
                self.app.notify(f"Export mislukt: {error_msg}", severity="error", timeout=4.0)
