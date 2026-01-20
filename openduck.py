from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Header,
    Footer,
    TextArea,
    DataTable,
    DirectoryTree,
    Static,
)
from textual.screen import ModalScreen
from textual.events import MouseDown
from pathlib import Path
from datetime import datetime
from typing import Iterable
import duckdb
import os


# =====================
# Config
# =====================

CWD = Path(os.getcwd())
HISTORY_FILE = CWD / "duckcli_history.sql"

DUCKDB_EXTENSIONS = {
    # columnar / structured
    ".parquet",
    ".arrow",
    ".duckdb",

    # text
    ".csv",
    ".csv.gz",
    ".json",
    ".jsonl",

    # sqlite
    ".sqlite",
    ".sqlite3",
    ".db",

    # excel
    ".xlsx",
    ".xls",
}


# =====================
# Helpers
# =====================

def is_duckdb_file(path: Path) -> bool:
    return any(path.name.lower().endswith(ext) for ext in DUCKDB_EXTENSIONS)


def sql_for_file(path: Path) -> str:
    p = path.as_posix()
    suffix = path.suffix.lower()

    # DuckDB database
    if suffix == ".duckdb":
        return f"""ATTACH '{p}' AS other;
SHOW TABLES;
"""

    # SQLite databases
    if suffix in {".sqlite", ".sqlite3", ".db"}:
        return f"""INSTALL sqlite;
LOAD sqlite;

ATTACH '{p}' AS sqlite_db (TYPE SQLITE);
SHOW TABLES FROM sqlite_db;
"""

    # Excel
    if suffix in {".xlsx", ".xls"}:
        return f"""SELECT *
FROM read_excel('{p}')
LIMIT 100;
"""

    # CSV
    if suffix in {".csv", ".csv.gz"}:
        return f"""SELECT *
FROM read_csv_auto('{p}')
LIMIT 100;
"""

    # JSON
    if suffix in {".json", ".jsonl"}:
        return f"""SELECT *
FROM read_json_auto('{p}')
LIMIT 100;
"""

    # Everything else DuckDB can infer
    return f"""SELECT *
FROM '{p}'
LIMIT 100;
"""


def append_history(sql: str):
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write("\n-- ----------------------------------------\n")
        f.write(f"-- {datetime.now().isoformat()}\n")
        f.write(sql.strip())
        f.write("\n")


# =====================
# Custom Widgets
# =====================

class DuckTree(DirectoryTree):
    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir() or is_duckdb_file(p)]


# =====================
# Menu Screens
# =====================

class MainMenu(ModalScreen):
    def compose(self) -> ComposeResult:
        yield Static(
            "\n[M] Close menu\n[Q] Quit\n",
            id="menu",
        )

    async def on_key(self, event):
        if event.key.lower() == "q":
            self.app.exit()
        if event.key.lower() in {"m", "escape"}:
            self.dismiss()


class FileContextMenu(ModalScreen):
    def __init__(self, path: Path):
        super().__init__()
        self.path = path

    def compose(self) -> ComposeResult:
        yield Static(
            f"""
File: {self.path.name}

[P] Preview
[D] Describe
[I] Insert path
[C] Cancel
""",
            id="menu",
        )

    async def on_key(self, event):
        query_area = self.app.query_one("#query", TextArea)
        p = self.path.as_posix()
        key = event.key.lower()

        if key == "p":
            query_area.text = sql_for_file(self.path)
            self.dismiss()

        elif key == "d":
            query_area.text = f"DESCRIBE SELECT * FROM '{p}';"
            self.dismiss()

        elif key == "i":
            query_area.insert(p)
            self.dismiss()

        elif key in {"c", "escape"}:
            self.dismiss()


# =====================
# Main App
# =====================

class DuckCLI(App):
    CSS = """
    Screen {
        layout: horizontal;
    }
    DirectoryTree {
        width: 30%;
    }
    TextArea {
        height: 30%;
    }
    DataTable {
        height: 1fr;
    }
    #menu {
        padding: 2;
        background: $surface;
        border: tall $primary;
    }
    """

    BINDINGS = [
        ("ctrl+enter", "run_query", "Run query"),
        ("ctrl+m", "menu", "Menu"),
        ("ctrl+q", "quit", "Quit"),
    ]

    def __init__(self):
        super().__init__()
        self.con = duckdb.connect()
        self.table = DataTable()

    def compose(self) -> ComposeResult:
        yield Header()

        yield Horizontal(
            # Gebruik de custom DuckTree voor filtering
            DuckTree(CWD, id="files"),
            Vertical(
                TextArea(
                    id="query",
                    placeholder="Write SQL here…  (Ctrl+Enter to run)",
                    language="sql",  # Syntax highlighting
                ),
                self.table,
            ),
        )

        yield Footer()

    def on_mount(self):
        self.table.cursor_type = "row"
        self.table.zebra_stripes = True

        if HISTORY_FILE.exists():
            text = HISTORY_FILE.read_text(encoding="utf-8")
            if "-- ----------------------------------------" in text:
                last = text.strip().split("-- ----------------------------------------")[-1]
                self.query_one("#query", TextArea).text = last.strip()

    # -----------------
    # Actions
    # -----------------

    async def action_menu(self):
        await self.push_screen(MainMenu())

    async def action_quit(self):
        self.exit()

    async def action_run_query(self):
        query_area = self.query_one("#query", TextArea)
        sql = query_area.text.strip()

        if not sql:
            return

        append_history(sql)
        self.table.clear(columns=True)

        try:
            result = self.con.execute(sql)
            
            # Check of het resultaat data bevat via description
            if not result.description:
                self.table.add_columns("Info")
                self.table.add_row("Query executed successfully (no result set).")
                return

            # Kolommen ophalen
            cols = [desc[0] for desc in result.description]
            
            # Data ophalen (fetchall is native en snel zonder pandas)
            rows = result.fetchall()

            if not rows:
                self.table.add_columns("Result")
                self.table.add_row("No rows returned")
                return

            # Voeg kolommen toe
            self.table.add_columns(*cols)

            # Voeg data toe (converteer waarden naar str voor DataTable)
            for row in rows:
                self.table.add_row(*[str(v) if v is not None else "NULL" for v in row])

        except Exception as e:
            self.table.clear(columns=True)
            self.table.add_columns("Error")
            self.table.add_row(str(e))

    # -----------------
    # Directory actions
    # -----------------

    async def on_directory_tree_file_selected(
        self, event: DirectoryTree.FileSelected
    ):
        path = Path(event.path)
        if not is_duckdb_file(path):
            return

        query_area = self.query_one("#query", TextArea)
        query_area.text = sql_for_file(path)
        query_area.focus()

    async def on_mouse_down(self, event: MouseDown):
        if event.button != 3:  # right click
            return

        hit = self.get_widget_at(event.screen_x, event.screen_y)
        if not hit:
            return

        widget = hit[0]
        
        # Check if widget is DirectoryTree or subclass
        if not isinstance(widget, DirectoryTree):
            return

        meta = event.style.meta or {}
        node_id = meta.get("node")
        if node_id is None:
            return

        node = widget.get_node(node_id)
        if not node or not isinstance(node.data, Path):
            return

        path = node.data
        if not is_duckdb_file(path):
            return

        await self.push_screen(FileContextMenu(path))

    # -----------------
    # Table actions
    # -----------------

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected):
        """Sorteer de tabel bij klikken op header."""
        self.table.sort(event.column_key)


if __name__ == "__main__":
    DuckCLI().run()