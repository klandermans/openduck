"""Data table widget for query results with copy and keyboard bindings."""

import logging
from textual.binding import Binding
from textual.widgets import DataTable

from openduck.core.clipboard import copy_to_system_clipboard

logger = logging.getLogger(__name__)


class ResultsTable(DataTable):
    """DataTable for displaying SQL query results with system clipboard row/table copying."""

    BINDINGS = [
        *DataTable.BINDINGS,
        Binding("ctrl+c,super+c", "copy_row", "Copy Row", show=False),
        Binding("ctrl+a,super+a", "copy_table", "Copy All", show=False),
    ]

    def action_copy_row(self) -> None:
        """Copy highlighted row or cell to clipboard."""
        try:
            if self.cursor_type == "row" and self.cursor_row is not None:
                row_data = self.get_row_at(self.cursor_row)
                text = "\t".join(str(v) if v is not None else "" for v in row_data)
                copy_to_system_clipboard(text)
                self.app.copy_to_clipboard(text)
                self.notify(f"Rij gekopieerd ({len(row_data)} kolommen)", timeout=1.5)
            elif self.cursor_type == "cell" and self.cursor_coordinate is not None:
                cell_data = self.get_cell_at(self.cursor_coordinate)
                text = str(cell_data) if cell_data is not None else ""
                copy_to_system_clipboard(text)
                self.app.copy_to_clipboard(text)
                self.notify("Cel gekopieerd naar klembord", timeout=1.5)
        except Exception as e:
            logger.error(f"Error copying table row: {e}")

    def action_copy_table(self) -> None:
        """Copy entire table with column headers as TSV."""
        try:
            lines = []
            headers = [col.label.plain for col in self.columns.values()]
            if headers:
                lines.append("\t".join(headers))
            for row_idx in range(self.row_count):
                row_data = self.get_row_at(row_idx)
                lines.append("\t".join(str(v) if v is not None else "" for v in row_data))
            text = "\n".join(lines)
            copy_to_system_clipboard(text)
            self.app.copy_to_clipboard(text)
            self.notify(f"Tabel gekopieerd ({self.row_count} rijen)", timeout=1.5)
        except Exception as e:
            logger.error(f"Error copying entire table: {e}")
