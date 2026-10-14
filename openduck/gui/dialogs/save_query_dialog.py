"""Modal dialog for saving named queries."""

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static

from openduck.core.config import save_query


class SaveQueryDialog(ModalScreen):
    """Modal dialog to save current query text with a custom name."""

    def __init__(self, query_text: str):
        super().__init__()
        self.query_text = query_text

    def compose(self) -> ComposeResult:
        with Vertical(id="save-dialog"):
            yield Static("Save Query", id="dialog-title")
            yield Input(placeholder="Enter query name...", id="query-name")
            yield Static("Query:", id="query-label")
            yield Static(self.query_text[:100] + ("..." if len(self.query_text) > 100 else ""), id="query-preview")
            with Horizontal():
                yield Button("Save", variant="primary", id="btn-save")
                yield Button("Cancel", id="btn-cancel")

    def on_mount(self):
        self.query_one("#query-name").focus()

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "btn-cancel":
            self.dismiss()
        elif event.button.id == "btn-save":
            name = self.query_one("#query-name", Input).value
            if name.strip():
                save_query(name, self.query_text)
                self.dismiss({"saved": True, "name": name})
            else:
                self.query_one("#query-name").value = ""
                self.query_one("#query-name").focus()
