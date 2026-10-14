"""Modal dialog for adding database connections (MySQL / MSSQL)."""

from datetime import datetime
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Select, Static


class AddConnectionDialog(ModalScreen):
    """Modal dialog to add a MySQL or MSSQL connection."""

    def __init__(self, existing_conn: dict = None):
        super().__init__()
        self.existing_conn = existing_conn

    def compose(self) -> ComposeResult:
        with Vertical(id="conn-dialog"):
            yield Static("Add Database Connection", id="conn-dialog-title")
            yield Static("Type:")
            yield Select(
                [("MySQL", "mysql"), ("MSSQL", "mssql")],
                prompt="Select type",
                id="conn-type",
                allow_blank=False,
                value="mysql",
            )
            yield Input(placeholder="Display name...", id="conn-display-name")
            yield Input(placeholder="Host (e.g. localhost)", id="conn-host", value="localhost")
            yield Input(placeholder="Port", id="conn-port", value="3306")
            yield Input(placeholder="Username", id="conn-user")
            yield Input(placeholder="Password", id="conn-password", password=True)
            yield Input(placeholder="Database name", id="conn-database")
            with Horizontal():
                yield Button("Connect", variant="primary", id="btn-connect")
                yield Button("Cancel", id="btn-conn-cancel")
            yield Static("", id="conn-status")

    def on_mount(self):
        self.query_one("#conn-display-name").focus()
        if self.existing_conn:
            self.query_one("#conn-type", Select).value = self.existing_conn["type"]
            self.query_one("#conn-display-name", Input).value = self.existing_conn.get("display_name", "")
            self.query_one("#conn-host", Input).value = self.existing_conn.get("host", "localhost")
            self.query_one("#conn-port", Input).value = str(self.existing_conn.get("port", "3306"))
            self.query_one("#conn-user", Input).value = self.existing_conn.get("user", "")
            self.query_one("#conn-password", Input).value = self.existing_conn.get("password", "")
            self.query_one("#conn-database", Input).value = self.existing_conn.get("database", "")

    def on_select_changed(self, event: Select.Changed):
        if event.select.id == "conn-type":
            port_input = self.query_one("#conn-port", Input)
            if event.value == "mysql" and port_input.value in ("1433", ""):
                port_input.value = "3306"
            elif event.value == "mssql" and port_input.value in ("3306", ""):
                port_input.value = "1433"

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "btn-conn-cancel":
            self.dismiss()
        elif event.button.id == "btn-connect":
            conn_info = {
                "id": self.existing_conn["id"] if self.existing_conn else f"conn_{int(datetime.now().timestamp() * 1000)}",
                "display_name": self.query_one("#conn-display-name", Input).value.strip(),
                "type": self.query_one("#conn-type", Select).value,
                "host": self.query_one("#conn-host", Input).value.strip(),
                "port": int(self.query_one("#conn-port", Input).value.strip() or "0"),
                "user": self.query_one("#conn-user", Input).value.strip(),
                "password": self.query_one("#conn-password", Input).value,
                "database": self.query_one("#conn-database", Input).value.strip(),
            }
            if not conn_info["display_name"]:
                conn_info["display_name"] = f"{conn_info['type']}://{conn_info['host']}/{conn_info['database']}"
            if not all([conn_info["host"], conn_info["database"]]):
                self.query_one("#conn-status").update("[red]Host and database are required[/red]")
                return
            self.dismiss(conn_info)
