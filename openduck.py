"""
DuckCLI Features:
- Async Execution: Query's draaien in een aparte thread; interface hangt niet.
- Metadata Footer: Toont uitvoeringstijd en aantal resultaten onder de tabel.
- Multi-Tab Support: Open meerdere bestanden of query's tegelijk.
- File Filtering: DirectoryTree toont alleen ondersteunde bestanden.
- Save Queries: Save queries with names to openduck.json
- History: Store query history in openduck.json
"""

import logging
import datetime as dt_module
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, Middle, ScrollableContainer
from textual.widgets import (
    Header, Footer, TextArea, DataTable, DirectoryTree,
    Static, Input, Button, TabbedContent, TabPane, ListView, ListItem, Label,
    Tree, Select
)
from textual.events import MouseEvent, Click, MouseDown, MouseMove, MouseUp
from textual.screen import ModalScreen
from pathlib import Path
from datetime import datetime
from typing import Iterable, List, Any, Optional
import duckdb
import os
import asyncio
import time
import json

# Configure logging to write to current directory with improved format
logging.basicConfig(
    filename='openduck_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    filemode='a'
)
logger = logging.getLogger(__name__)
import random

# =====================
# Config & Helpers
# =====================
CWD = Path(os.getcwd())
CONFIG_FILE = CWD / "openduck.json"
DUCKDB_EXTENSIONS = {
    ".parquet", ".arrow", ".duckdb", ".csv", ".csv.gz",
    ".json", ".jsonl", ".sqlite", ".sqlite3", ".db", ".xlsx", ".xls"
}

def is_duckdb_file(path: Path) -> bool:
    return path.is_file() and any(path.name.lower().endswith(ext) for ext in DUCKDB_EXTENSIONS)

def sql_for_file(path: Path) -> str:
    p, s = path.as_posix(), path.suffix.lower()
    if s == ".duckdb": return f"ATTACH '{p}' AS other;\nSHOW TABLES;"
    if s in {".sqlite", ".sqlite3", ".db"}:
        return f"INSTALL sqlite;\nLOAD sqlite;\nATTACH '{p}' AS sqlite_db (TYPE SQLITE);\nSHOW TABLES FROM sqlite_db;"
    if s in {".xlsx", ".xls"}: return f"SELECT * FROM read_excel('{p}') LIMIT 100;"
    if s in {".csv", ".csv.gz"}: return f"SELECT * FROM read_csv_auto('{p}') LIMIT 100;"
    if s in {".json", ".jsonl"}: return f"SELECT * FROM read_json_auto('{p}') LIMIT 100;"
    return f"SELECT * FROM '{p}' LIMIT 100;"

def load_config():
    """Load config from openduck.json, create if doesn't exist"""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            if "connections" not in config:
                config["connections"] = []
            return config
    else:
        default_config = {
            "history": [],
            "saved_queries": [],
            "connections": []
        }
        save_config(default_config)
        return default_config

def save_config(config):
    """Save config to openduck.json"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def add_to_history(sql: str):
    """Add a query to history in the config file"""
    config = load_config()
    timestamp = datetime.now().isoformat()
    config["history"].append({
        "sql": sql.strip(),
        "timestamp": timestamp
    })
    # Unlimited history - no limit applied
    save_config(config)
    return config  # Return updated config for potential use by caller

def save_query(name: str, sql: str):
    """Save a query with a name to the config file"""
    config = load_config()
    timestamp = datetime.now().isoformat()
    # Check if query with same name already exists and update it
    for i, query in enumerate(config["saved_queries"]):
        if query["name"] == name:
            config["saved_queries"][i] = {
                "name": name,
                "sql": sql.strip(),
                "timestamp": timestamp
            }
            save_config(config)
            return config
    # If not found, add as new query
    config["saved_queries"].append({
        "name": name,
        "sql": sql.strip(),
        "timestamp": timestamp
    })
    save_config(config)
    return config

def save_connection(conn_info: dict):
    """Save a database connection to config"""
    config = load_config()
    for i, c in enumerate(config["connections"]):
        if c["id"] == conn_info["id"]:
            config["connections"][i] = conn_info
            save_config(config)
            return config
    config["connections"].append(conn_info)
    save_config(config)
    return config

def delete_connection(conn_id: str):
    """Delete a database connection from config"""
    config = load_config()
    config["connections"] = [c for c in config["connections"] if c["id"] != conn_id]
    save_config(config)
    return config

# =====================
# UI Components
# =====================
MATRIX_CHARS = "ｱｲｳｴｵｶｷｸｹｺｻｼｽｾｿﾀﾁﾂﾃﾄﾅﾆﾇﾈﾉ0123456789"
# ANSI 256: 82=#87ff00, 46=#00ff00, 40=#00d700, 34=#00af00, 28=#008700, 22=#005f00
MATRIX_SHADES = ["#87ff00", "#00ff00", "#00d700", "#00af00", "#008700", "#005f00"]

class MatrixScreen(ModalScreen):
    DEFAULT_CSS = """
    MatrixScreen { background: black; }
    #matrix-canvas { width: 100%; height: 100%; background: black; }
    """
    def __init__(self):
        super().__init__()
        self._drops = []
        self._grid = []
        self._colors = []
        self._timer = None

    def compose(self) -> ComposeResult:
        yield Static("", id="matrix-canvas")

    def on_mount(self) -> None:
        cols = self.screen.size.width
        rows = self.screen.size.height
        self._drops = [random.randint(0, rows) for _ in range(cols)]
        # Persistent grid - characters stay until overwritten
        self._grid = [[" "] * cols for _ in range(rows)]
        self._colors = [["#005f00"] * cols for _ in range(rows)]
        self._timer = self.set_interval(0.03, self._tick)

    def _tick(self) -> None:
        cols = self.screen.size.width
        rows = self.screen.size.height
        # Resize grids if terminal changed
        while len(self._drops) < cols:
            self._drops.append(0)
        self._drops = self._drops[:cols]
        while len(self._grid) < rows:
            self._grid.append([" "] * cols)
            self._colors.append(["#005f00"] * cols)
        self._grid = self._grid[:rows]
        self._colors = self._colors[:rows]
        for r in range(rows):
            while len(self._grid[r]) < cols:
                self._grid[r].append(" ")
                self._colors[r].append("#005f00")
            self._grid[r] = self._grid[r][:cols]
            self._colors[r] = self._colors[r][:cols]
        # Draw drops - characters persist on the grid like the original
        for i in range(cols):
            if random.random() > 0.97:
                self._drops[i] = 0
            for j, shade in enumerate(MATRIX_SHADES):
                y = self._drops[i] - j
                if 0 <= y < rows:
                    self._grid[y][i] = random.choice(MATRIX_CHARS)
                    self._colors[y][i] = shade
            self._drops[i] += 1
            if self._drops[i] >= rows:
                self._drops[i] = 0
        # Render
        lines = []
        for r in range(rows):
            parts = []
            for c in range(cols):
                ch = self._grid[r][c]
                if ch != " ":
                    parts.append(f"[{self._colors[r][c]}]{ch}[/]")
                else:
                    parts.append(" ")
            lines.append("".join(parts))
        self.query_one("#matrix-canvas").update("\n".join(lines))

    def on_key(self) -> None:
        if self._timer:
            self._timer.stop()
        self.dismiss()

    def on_click(self) -> None:
        if self._timer:
            self._timer.stop()
        self.dismiss()

LOGO_RAW = (
    "   ___                       _            _    \n"
    "  / _ \\ _ __   ___ _ __   __| |_   _  ___| | __\n"
    " | | | | '_ \\ / _ \\ '_ \\ / _` | | | |/ __| |/ /\n"
    " | |_| | |_) |  __/ | | | (_| | |_| | (__|   < \n"
    "  \\___/| .__/ \\___|_| |_|\\__,_|\\__,_|\\___|_|\\_\\\n"
    "       |_|                                     "
)

LOLCAT_COLORS = [
    "#ff0000", "#ff4400", "#ff8800", "#ffcc00", "#ffff00", "#88ff00",
    "#00ff00", "#00ff88", "#00ffff", "#0088ff", "#0000ff", "#4400ff",
    "#8800ff", "#cc00ff", "#ff00ff", "#ff0088",
]

def lolcat(text: str) -> str:
    """Apply rainbow colors to text, Rich markup style."""
    from rich.text import Text
    rich_text = Text(text)
    lines = text.split("\n")
    pos = 0
    for row, line in enumerate(lines):
        for col, ch in enumerate(line):
            if ch != " ":
                color = LOLCAT_COLORS[(col + row * 3) % len(LOLCAT_COLORS)]
                rich_text.stylize(color, pos, pos + 1)
            pos += 1
        pos += 1  # newline
    return rich_text

DUCK_ART = r"""                  (o.                   H
      _o)         |  . :             (o]H
  \\\__/      \\\_|  : :.        \\\_\  H
  <____).....<_____).:.::.......<_____).H"""

class AboutScreen(ModalScreen):
    DEFAULT_CSS = """
    #easter-egg { color: $text-disabled; }
    #duck-art { color: $accent; }
    """
    def compose(self) -> ComposeResult:
        with Middle():
            with Vertical(id="about-inner"):
                yield Static(lolcat(LOGO_RAW), id="about-title")
                yield Static("────────────────────────────────────────────────────")
                yield Static("[b]GitHub:[/b] github.com/bertatron/duckcli")
                yield Static("[b]Author:[/b] Bertatron")
                yield Static("[b]License:[/b] MIT")
                yield Static("\nKISS - Metadata & Async enabled.")
                yield Static("Don't click here", id="easter-egg")
                yield Static(DUCK_ART, id="duck-art")
                yield Static("\n[Press any key to close]", id="about-footer")

    def on_key(self) -> None: self.dismiss()

    def on_click(self, event: Click) -> None:
        if hasattr(event, 'widget') and event.widget and event.widget.id == "easter-egg":
            self.dismiss()
            self.app.push_screen(MatrixScreen())
        else:
            try:
                widget, _ = self.screen.get_widget_at(event.screen_x, event.screen_y)
                if widget.id == "easter-egg":
                    self.dismiss()
                    self.app.push_screen(MatrixScreen())
                    return
            except Exception:
                pass
            self.dismiss()

class ResizeHandle(Static):
    """A draggable handle to resize the sidebar."""
    DEFAULT_CSS = """
    ResizeHandle {
        width: 1;
        height: 100%;
        background: $primary;
    }
    ResizeHandle:hover {
        background: $accent;
    }
    """
    def __init__(self):
        super().__init__("┃")
        self._dragging = False

    def on_mouse_down(self, event: MouseDown) -> None:
        self._dragging = True
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        if self._dragging:
            # screen_x gives us the absolute position of the mouse
            screen_width = self.screen.size.width
            new_width = max(10, min(event.screen_x, screen_width - 20))
            sidebar = self.screen.query_one("#sidebar")
            sidebar.styles.width = new_width
            event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._dragging:
            self._dragging = False
            self.release_mouse()
            event.stop()

class DuckTree(DirectoryTree):
    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir() or is_duckdb_file(p)]

class DatabaseTree(Vertical):
    """Tree widget showing database connections and their tables with search functionality."""

    def __init__(self):
        super().__init__(id="db-tree")
        self.connections_data = {}
        self.original_tree = Tree("Databases", id="db-tree-inner")
        self.search_input = Input(placeholder="Search tables...", id="db-search-input")
        
    def compose(self) -> ComposeResult:
        yield self.search_input
        yield self.original_tree
        
    def on_mount(self):
        self.original_tree.root.expand()
        self._ensure_add_node()
        
    def _ensure_add_node(self):
        """Make sure the '+ Add connection' node is always the last leaf."""
        # Remove existing add-node if present
        for child in list(self.original_tree.root.children):
            if child.data and child.data.get("type") == "add_connection":
                child.remove()
        self.original_tree.root.add_leaf("+ Add connection", data={"type": "add_connection"})

    def add_connection_node(self, conn_info: dict, tables: list = None):
        conn_id = conn_info["id"]
        type_icon = "\U0001f42c" if conn_info["type"] == "mysql" else "\U0001f537"
        label = f"{type_icon} {conn_info['display_name']}"
        self.remove_connection_node(conn_id)
        self.connections_data[conn_id] = conn_info
        conn_node = self.original_tree.root.add(
            label,
            data={"type": "connection", "conn_id": conn_id},
            expand=True,
        )
        if tables:
            for table_name in sorted(tables):
                conn_node.add_leaf(
                    f"  {table_name}",
                    data={"type": "table", "conn_id": conn_id, "table": table_name, "database": conn_info["database"]},
                )
        else:
            conn_node.add_leaf("Loading...", data={"type": "loading", "conn_id": conn_id})
        self._ensure_add_node()

    def remove_connection_node(self, conn_id: str):
        for child in list(self.original_tree.root.children):
            if child.data and child.data.get("conn_id") == conn_id:
                child.remove()
                break
        self.connections_data.pop(conn_id, None)
        self._ensure_add_node()

    def filter_tables(self, search_term: str):
        """Filter the tree to show only tables that match the search term."""
        search_term = search_term.strip().lower()

        # Clear the tree and rebuild with filtered content
        self.original_tree.clear()

        for conn_id, conn_info in self.connections_data.items():
            type_icon = "\U0001f42c" if conn_info["type"] == "mysql" else "\U0001f537"
            label = f"{type_icon} {conn_info['display_name']}"

            # Filter tables based on search term (case insensitive)
            all_tables = conn_info.get("tables", [])
            if search_term:
                # Case-insensitive search
                filtered_tables = [table for table in all_tables if search_term in table.lower()]
            else:
                # Show all tables when search is empty
                filtered_tables = all_tables

            # Always add the connection node, but with filtered tables
            conn_node = self.original_tree.root.add(
                label,
                data={"type": "connection", "conn_id": conn_id},
                expand=True,
            )
            
            # Add tables based on filtering
            for table_name in sorted(filtered_tables):
                conn_node.add_leaf(
                    f"  {table_name}",
                    data={"type": "table", "conn_id": conn_id, "table": table_name, "database": conn_info["database"]},
                )
            
            # If search term exists but no tables match, add a "No matches" indicator
            if search_term and not filtered_tables:
                conn_node.add_leaf(
                    "  No matching tables",
                    data={"type": "no_match", "conn_id": conn_id},
                )

        self._ensure_add_node()
        self.original_tree.root.expand()

    def update_tables(self, conn_id: str, tables: list):
        conn_info = self.connections_data.get(conn_id)
        if conn_info:
            self.add_connection_node(conn_info, tables)


class QueryTab(Vertical):
    def __init__(self, sql: str = "", connection_id: str = None):
        super().__init__()
        self.initial_sql = sql
        self.full_data, self.column_names, self.col_states = [], [], {}
        self.running_task = None
        self.connection_id = connection_id

    def compose(self) -> ComposeResult:
        text_area = TextArea(
            self.initial_sql,
            language="sql",
            id="query-input",
            theme="css",  # Use the default theme which should support SQL
            soft_wrap=False,
            read_only=False,
            tab_behavior="indent"
        )
        # Explicitly set the language after creation to ensure highlighting
        text_area.language = "sql"
        yield text_area
        with Horizontal():  # Container for table and export sidebar
            yield DataTable(id="results-table")
            with Vertical(id="export-sidebar"):
                yield Button("↓CSV", id="export-csv", classes="export-btn")
                yield Button("↓XLSX", id="export-excel", classes="export-btn")
                yield Button("❌", id="cancel-query", classes="export-btn", tooltip="Cancel Query")
        yield Static("Ready", id="metadata-bar")

    def on_mount(self):
        t = self.query_one(DataTable)
        t.cursor_type, t.zebra_stripes = "row", True

class SaveQueryDialog(ModalScreen):
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

class ExportDialog(ModalScreen):
    def __init__(self, tab: 'QueryTab', export_type: str):
        super().__init__()
        self.tab = tab
        self.export_type = export_type

    def compose(self) -> ComposeResult:
        with Vertical(id="export-modal"):
            yield Static(f"Export {self.export_type.upper()}", id="export-modal-title")
            yield Input(placeholder="Enter filename...", id="export-filename")
            yield Static("Current query:", id="current-query-label")
            yield Static(self.tab.query_one(TextArea).text[:100] + ("..." if len(self.tab.query_one(TextArea).text) > 100 else ""), id="current-query-preview")
            with Horizontal():
                yield Button("Export", variant="primary", id="btn-export")
                yield Button("Cancel", id="btn-cancel")

    def on_mount(self):
        # Pre-populate filename based on query content
        query_text = self.tab.query_one(TextArea).text.strip()
        import re
        # Extract table name from query if possible
        table_match = re.search(r'FROM\s+(\w+)', query_text, re.IGNORECASE)
        base_name = table_match.group(1) if table_match else "query_result"
        # Sanitize filename
        base_name = re.sub(r'[^\w\-_]', '_', base_name)
        extension = ".csv" if self.export_type == "csv" else ".xlsx"
        default_filename = f"{base_name}{extension}"
        
        self.query_one("#export-filename").value = default_filename
        self.query_one("#export-filename").focus()

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "btn-cancel":
            self.dismiss()
        elif event.button.id == "btn-export":
            filename = self.query_one("#export-filename", Input).value
            if filename.strip():
                if self.export_type == "csv":
                    self.export_to_csv(filename)
                else:  # excel
                    self.export_to_excel(filename)
                self.dismiss({"exported": True, "filename": filename})
            else:
                self.query_one("#export-filename").value = ""
                self.query_one("#export-filename").focus()

    def export_to_csv(self, filename: str):
        """Export table data to CSV file"""
        import csv
        filepath = CWD / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write headers
            writer.writerow(self.tab.column_names)
            # Write data rows
            for row in self.tab.full_data:
                writer.writerow(row)

    def export_to_excel(self, filename: str):
        """Export table data to Excel file"""
        try:
            import pandas as pd
            filepath = CWD / filename
            
            # Create DataFrame from the data
            df = pd.DataFrame(self.tab.full_data, columns=self.tab.column_names)
            # Write to Excel
            df.to_excel(filepath, index=False)
        except ImportError:
            # Show error if pandas not available
            pass

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

class DuckCLI(App):
    ENABLE_MOUSE_SUPPORT = True
    
    CSS = """
    Screen { layout: horizontal; }
    #sidebar { width: 30; background: $surface; }
    #main-content { width: 1fr; }
    #saved-queries-tree, #history-tree { height: auto; max-height: 30%; }
    TabbedContent { width: 100%; height: 100%; }
    TextArea { height: 25%; border-bottom: tall $primary; }
    DataTable { height: 1fr; }
    #export-sidebar { width: auto; margin-left: 1; width: 8; background: $panel; border: round $primary; padding: 0; }
    .export-btn { margin: 0; padding: 0; height: 2; width: 100%; }
    #metadata-bar {
        height: 1;
        background: $primary;
        color: $text;
        padding: 0 1;
    }
    #menu { padding: 1 2; background: $surface; border: tall $primary; width: 50; height: auto; }
    #menu-title { text-style: bold; margin-bottom: 1; }
    #filter-input { margin-bottom: 1; }
    Button { margin: 0 1; min-width: 10; }
    AboutScreen { align: center middle; background: rgba(0, 0, 0, 0.7); }
    #about-inner { width: 64; height: auto; background: $surface; border: tall $primary; padding: 2 4; }
    #about-title { text-align: center; color: $accent; }
    #about-footer { text-align: center; color: $text-disabled; }
    #save-dialog { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
    #dialog-title { text-style: bold; margin-bottom: 1; text-align: center; }
    #query-preview { height: 3; border: solid $primary; padding: 1; }
    #export-modal { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
    #export-modal-title { text-style: bold; margin-bottom: 1; text-align: center; }
    #export-filename { margin-bottom: 1; }
    #db-tree { height: auto; max-height: 40%; }
    #db-search-input { margin-bottom: 1; }
    #db-tree-inner { height: 1fr; }
    #conn-dialog { width: 60; height: auto; background: $surface; border: tall $primary; padding: 2; }
    #conn-dialog-title { text-style: bold; margin-bottom: 1; text-align: center; }
    #conn-status { height: 1; margin-top: 1; }
    AddConnectionDialog { align: center middle; background: rgba(0, 0, 0, 0.7); }
    """
    BINDINGS = [
        ("ctrl+enter", "run_query", "Run"),
        ("ctrl+s", "save_query", "Save Query"),
        ("ctrl+w", "close_tab", "Close Tab"),
        ("ctrl+d", "disconnect_db", "Disconnect DB"),
        ("ctrl+a", "about", "About"),
        ("ctrl+q", "quit", "Quit")
    ]

    def __init__(self):
        super().__init__()
        self.con = duckdb.connect()
        self.config = load_config()
        self.db_connections: dict = {}

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="sidebar"):
                yield DuckTree(CWD, id="files")
                yield DatabaseTree()
                yield Tree("Saved Queries", id="saved-queries-tree")
                yield Tree("History", id="history-tree")
            yield ResizeHandle()
            with Vertical(id="main-content"):
                yield TabbedContent(id="tabs")
        yield Header()
        yield Footer()

    def on_mount(self):
        self.load_history_list()
        self.load_saved_queries_list()
        self.load_saved_connections()

        sql = ""
        if self.config["history"]:
            sql = self.config["history"][-1]["sql"]
        self.run_worker(self.add_new_tab("Main", sql))

    def load_saved_connections(self):
        """Load saved connections from config and attempt to connect."""
        for conn_info in self.config.get("connections", []):
            self.run_worker(self.connect_database(conn_info))

    async def connect_database(self, conn_info: dict):
        """Establish a database connection and populate tree."""
        conn_id = conn_info["id"]
        db_tree = self.query_one("#db-tree", DatabaseTree)
        db_tree.add_connection_node(conn_info)
        try:
            if conn_info["type"] == "mysql":
                tables = await self._connect_mysql(conn_info)
            elif conn_info["type"] == "mssql":
                tables = await self._connect_mssql(conn_info)
            else:
                raise ValueError(f"Unknown type: {conn_info['type']}")
            db_tree.update_tables(conn_id, tables)
        except Exception as e:
            db_tree.remove_connection_node(conn_id)
            err_node = db_tree.root.add(
                f"[red]ERR: {conn_info['display_name']}[/red]",
                data={"type": "error", "conn_id": conn_id},
            )
            err_node.add_leaf(f"[red]{str(e)[:60]}[/red]")
            self.db_connections.pop(conn_id, None)

    async def _connect_mysql(self, conn_info: dict) -> list:
        conn_id = conn_info["id"]
        alias = conn_id.replace("-", "_")
        attach_str = (
            f"host={conn_info['host']} "
            f"user={conn_info['user']} "
            f"port={conn_info['port']} "
            f"database={conn_info['database']} "
            f"password={conn_info['password']}"
        )
        def do_attach():
            self.con.execute("INSTALL mysql; LOAD mysql;")
            try:
                self.con.execute(f"DETACH {alias}")
            except Exception:
                pass
            self.con.execute(f"ATTACH '{attach_str}' AS {alias} (TYPE MYSQL)")
            result = self.con.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{alias}'"
            ).fetchall()
            return [row[0] for row in result]
        tables = await asyncio.to_thread(do_attach)
        self.db_connections[conn_id] = {"type": "mysql", "alias": alias}
        return tables

    async def _connect_mssql(self, conn_info: dict) -> list:
        import pymssql
        conn_id = conn_info["id"]
        def do_connect():
            conn = pymssql.connect(
                server=conn_info["host"],
                port=conn_info["port"],
                user=conn_info["user"],
                password=conn_info["password"],
                database=conn_info["database"],
                login_timeout=10,
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
            tables = [row[0] for row in cursor.fetchall()]
            return conn, tables
        conn, tables = await asyncio.to_thread(do_connect)
        self.db_connections[conn_id] = {"type": "mssql", "connection": conn}
        return tables

    def _disconnect(self, conn_id: str):
        conn_meta = self.db_connections.pop(conn_id, None)
        if conn_meta:
            if conn_meta["type"] == "mysql":
                try:
                    self.con.execute(f"DETACH {conn_meta['alias']}")
                except Exception:
                    pass
            elif conn_meta["type"] == "mssql":
                try:
                    conn_meta["connection"].close()
                except Exception:
                    pass
        db_tree = self.query_one("#db-tree", DatabaseTree)
        db_tree.remove_connection_node(conn_id)
        delete_connection(conn_id)
        self.config = load_config()

    def action_disconnect_db(self):
        try:
            db_tree = self.query_one("#db-tree", DatabaseTree)
            node = db_tree.cursor_node
            if node and node.data:
                conn_id = node.data.get("conn_id")
                if conn_id:
                    self._disconnect(conn_id)
        except Exception:
            pass

    async def add_new_tab(self, name: str, sql: str, run: bool = False, connection_id: str = None):
        tabs = self.query_one("#tabs", TabbedContent)
        tab_id = f"t{int(datetime.now().timestamp() * 1000)}"
        await tabs.add_pane(TabPane(f"{name} ✕", QueryTab(sql, connection_id=connection_id), id=tab_id))
        tabs.active = tab_id
        if run: self.set_timer(0.1, self.action_run_query)

    async def action_run_query(self):
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active: 
            logging.debug("No active tab found")
            return
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        sql = tab.query_one(TextArea).text.strip()
        if not sql: 
            logging.debug("No SQL query to execute")
            return

        logging.debug(f"Executing query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
        
        # Update config after adding to history
        add_to_history(sql)
        self.config = load_config()  # Reload config to ensure it's up to date
        self.load_history_list()  # Refresh the history list in UI

        tbl = tab.query_one(DataTable)
        meta = tab.query_one("#metadata-bar")
        tbl.loading = True
        meta.update("Executing...")

        # Track this task so it can be cancelled
        tab.running_task = asyncio.current_task()
        logging.debug("Query execution started, task tracked")

        conn_id = tab.connection_id
        conn_meta = self.db_connections.get(conn_id) if conn_id else None
        logging.debug(f"Connection info: {conn_meta['type'] if conn_meta else 'None'}")

        def execute():
            start = time.perf_counter()
            logging.debug("Executing SQL query in thread")
            if conn_meta and conn_meta["type"] == "mssql":
                mssql_conn = conn_meta["connection"]
                cursor = mssql_conn.cursor()
                cursor.execute(sql)
                if not cursor.description:
                    logging.debug("Query returned no results (MS SQL)")
                    return [], ["Info"], None, time.perf_counter() - start
                cols = [d[0] for d in cursor.description]
                data = [list(r) for r in cursor.fetchall()]
                logging.debug(f"MS SQL query executed: {len(data)} rows, {len(cols)} columns")
                return data, cols, None, time.perf_counter() - start
            else:
                cursor = self.con.cursor()
                res = cursor.execute(sql)
                if not res.description:
                    duration = time.perf_counter() - start
                    logging.debug("Query returned no results (DuckDB)")
                    return [], ["Info"], [["Success"]], duration
                cols = [d[0] for d in res.description]
                data = [list(r) for r in res.fetchall()]
                duration = time.perf_counter() - start
                logging.debug(f"DuckDB query executed: {len(data)} rows, {len(cols)} columns")
                return data, cols, None, duration

        try:
            data, cols, _, duration = await asyncio.to_thread(execute)
            tab.column_names = cols
            tab.full_data = data
            tab.col_states = {i: {"filter": "", "sort": None} for i in range(len(cols))}
            logging.debug(f"Setting up table with {len(cols)} columns and {len(data)} rows")
            self.refresh_tab_table(tab)
            conn_label = ""
            if conn_meta:
                conn_label = f" | via {conn_meta['type'].upper()}"
            meta.update(f"Rows: {len(data)} | Time: {duration:.4f}s{conn_label} | Finished: {datetime.now().strftime('%H:%M:%S')}")
            logging.debug(f"Query completed successfully: {len(data)} rows in {duration:.4f}s")
        except asyncio.CancelledError:
            logging.debug("Query was cancelled")
            meta.update("Query cancelled")
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            tbl.clear(columns=True)
            tbl.add_column("Error")
            tbl.add_row(str(e))
            meta.update("Error occurred")
        finally:
            tbl.loading = False
            tab.running_task = None  # Clear the running task reference
            logging.debug("Query execution completed, task reference cleared")

    def action_save_query(self):
        """Save the current query with a name"""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active: return
        
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        sql = tab.query_one(TextArea).text.strip()
        if not sql: return
        
        def handle_result(result):
            if result and result.get("saved"):
                # Reload the config and refresh the UI
                self.config = load_config()
                self.load_saved_queries_list()
        
        self.push_screen(SaveQueryDialog(sql), handle_result)

    def load_query_in_current_tab(self, sql: str):
        """Load a query into the current active tab"""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active: return
        
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        textarea = tab.query_one(TextArea)
        textarea.text = sql

    def load_history_list(self):
        """Load history into the history tree"""
        tree = self.query_one("#history-tree", Tree)
        tree.root.expand()
        # Remove old children
        for child in list(tree.root.children):
            child.remove()
        for i, item in enumerate(reversed(self.config["history"])):
            timestamp = datetime.fromisoformat(item["timestamp"]).strftime("%H:%M:%S")
            sql_preview = item["sql"][:50] + ("..." if len(item["sql"]) > 50 else "")
            label = f"[{timestamp}] {sql_preview}"
            tree.root.add_leaf(label, data={"type": "history", "sql": item["sql"]})

    def load_saved_queries_list(self):
        """Load saved queries into the saved queries tree"""
        tree = self.query_one("#saved-queries-tree", Tree)
        tree.root.expand()
        for child in list(tree.root.children):
            child.remove()
        for i, item in enumerate(self.config["saved_queries"]):
            timestamp = datetime.fromisoformat(item["timestamp"]).strftime("%d/%m %H:%M")
            label = f"{item['name']} [{timestamp}]"
            tree.root.add_leaf(label, data={"type": "saved", "sql": item["sql"]})

    def refresh_tab_table(self, tab: QueryTab):
        logging.debug(f"Refreshing table: {len(tab.full_data)} rows, {len(tab.column_names)} columns")
        
        tbl, data = tab.query_one(DataTable), [r[:] for r in tab.full_data]
        
        # Apply filters first
        for i, s in tab.col_states.items():
            if s["filter"]:
                f = s["filter"].lower()
                initial_count = len(data)
                data = [r for r in data if f in str(r[i]).lower()]
                logging.debug(f"Applied filter on column {i}: {initial_count} -> {len(data)} rows")
        
        # Apply sorting
        sort_applied = False
        for i, s in tab.col_states.items():
            if s["sort"]:
                initial_count = len(data)
                data.sort(key=lambda x: x[i] if x[i] is not None else "", reverse=(s["sort"]=="desc"))
                sort_applied = True
                logging.debug(f"Applied sort on column {i} ({tab.column_names[i] if i < len(tab.column_names) else 'N/A'}): {s['sort']}, {initial_count} rows sorted")
                break

        tbl.clear(columns=True)
        # Add columns with sort indicators
        for i, name in enumerate(tab.column_names):
            s = tab.col_states[i]
            sort_indicator = ''
            if s['sort'] == 'asc':
                sort_indicator = ' ▲'
            elif s['sort'] == 'desc':
                sort_indicator = ' ▼'
            
            filter_indicator = ' 🔍' if s['filter'] else ''
            lbl = f"{name}{sort_indicator}{filter_indicator}"
            tbl.add_column(lbl, key=str(i))
            logging.debug(f"Added column {i}: {lbl}")
        
        # Add data rows
        logging.debug(f"Adding {len(data)} data rows to table")
        for r in data: 
            tbl.add_row(*[str(v) if v is not None else "NULL" for v in r])
        
        logging.debug(f"Table refresh completed: {len(data)} rows, {len(tab.column_names)} columns")

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected):
        tabs = self.query_one("#tabs", TabbedContent)
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        idx = int(event.column_key.value)
        
        logging.debug(f"Header selected: column index {idx}, column name {tab.column_names[idx] if idx < len(tab.column_names) else 'N/A'}")
        
        # Toggle sort direction: None -> ASC -> DESC -> None
        current_sort = tab.col_states[idx]["sort"]
        logging.debug(f"Current sort state for column {idx}: {current_sort}")
        
        if current_sort is None:
            new_sort = "asc"
        elif current_sort == "asc":
            new_sort = "desc"
        else:  # current_sort == "desc"
            new_sort = None
        
        logging.debug(f"New sort state for column {idx}: {new_sort}")
        
        # Clear all other sorts
        for i in tab.col_states:
            tab.col_states[i]["sort"] = None
        
        # Apply new sort
        tab.col_states[idx]["sort"] = new_sort
        logging.debug(f"Applied sort: {new_sort} to column {idx}, refreshing table")
        self.refresh_tab_table(tab)

    async def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        if is_duckdb_file(Path(event.path)):
            await self.add_new_tab(Path(event.path).name, sql_for_file(Path(event.path)), run=True)

    def action_about(self): self.push_screen(AboutScreen())
    def on_click(self, event: Click) -> None:
        """Handle clicks on tab close buttons (✕ in tab titles)."""
        from textual.widgets._tabbed_content import ContentTab
        try:
            widget, _ = self.screen.get_widget_at(event.screen_x, event.screen_y)
        except Exception:
            return
        if isinstance(widget, ContentTab) and widget.label_text.endswith(" ✕"):
            # Check if click was on the ✕ area (last 3 characters of the tab)
            relative_x = event.screen_x - widget.region.x
            if relative_x >= widget.region.width - 3:
                pane_id = ContentTab.sans_prefix(widget.id)
                self.query_one("#tabs", TabbedContent).remove_pane(pane_id)
                event.stop()

    def action_close_tab(self):
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active: tabs.remove_pane(tabs.active)

    def action_quit(self): self.exit()

    def on_tree_node_selected(self, event: Tree.NodeSelected):
        """Handle selection in database tree, saved queries tree, and history tree."""
        node = event.node
        if not node.data:
            return
        node_type = node.data.get("type")
        if node_type == "table":
            conn_id = node.data["conn_id"]
            table_name = node.data["table"]
            conn_meta = self.db_connections.get(conn_id)
            if conn_meta and conn_meta["type"] == "mysql":
                alias = conn_meta["alias"]
                sql = f"SELECT * FROM {alias}.{table_name} LIMIT 100;"
                self.run_worker(self.add_new_tab(table_name, sql, run=True))
            elif conn_meta and conn_meta["type"] == "mssql":
                sql = f"SELECT TOP 100 * FROM [{table_name}];"
                self.run_worker(self.add_new_tab(table_name, sql, run=True, connection_id=conn_id))
        elif node_type == "error":
            conn_id = node.data.get("conn_id")
            for c in self.config.get("connections", []):
                if c["id"] == conn_id:
                    def handle_retry(result):
                        if result:
                            save_connection(result)
                            self.config = load_config()
                            self.run_worker(self.connect_database(result))
                    self.push_screen(AddConnectionDialog(existing_conn=c), handle_retry)
                    break
        elif node_type == "add_connection":
            def handle_new_conn(result):
                if result:
                    save_connection(result)
                    self.config = load_config()
                    self.run_worker(self.connect_database(result))
            self.push_screen(AddConnectionDialog(), handle_new_conn)
        elif node_type in ("history", "saved"):
            sql = node.data.get("sql")
            if sql:
                self.load_query_in_current_tab(sql)

    def on_button_pressed(self, event: Button.Pressed):
        """Handle button presses"""
        if event.button.id in ["export-csv", "export-excel"]:
            # Find the current tab and its data
            tabs = self.query_one("#tabs", TabbedContent)
            if not tabs.active: 
                return
            
            tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
            if not tab.full_data or not tab.column_names:
                # No data to export
                meta = tab.query_one("#metadata-bar") if tab else None
                if meta:
                    meta.update("No data to export")
                return
            
            # Determine export type based on button
            export_type = "csv" if event.button.id == "export-csv" else "excel"
            
            def handle_export(result):
                if result and result.get("exported"):
                    filename = result["filename"]
                    meta = tab.query_one("#metadata-bar")
                    meta.update(f"Exported to {filename}")
            
            # Show export dialog
            self.push_screen(ExportDialog(tab, export_type), handle_export)
        
        elif event.button.id == "cancel-query":
            # Cancel the running query
            tabs = self.query_one("#tabs", TabbedContent)
            if not tabs.active: 
                return
            
            tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
            if tab.running_task and not tab.running_task.done():
                tab.running_task.cancel()
                meta = tab.query_one("#metadata-bar")
                meta.update("Query cancelled")
                tbl = tab.query_one(DataTable)
                tbl.loading = False
        
        elif event.button.id == "close-tab":
            # Close the current tab
            tabs = self.query_one("#tabs", TabbedContent)
            if tabs.active:
                tabs.remove_pane(tabs.active)
    
    def on_input_changed(self, event: Input.Changed):
        """Handle search input changes for database tables"""
        if event.input.id == "db-search-input":
            # Find the database tree and filter tables
            try:
                db_tree = self.query_one("#db-tree", DatabaseTree)
                db_tree.filter_tables(event.value)
            except:
                # If the element doesn't exist yet, ignore
                pass

if __name__ == "__main__":
    DuckCLI().run()