"""
DuckCLI Features:
- Async Execution: Query's draaien in een aparte thread; interface hangt niet.
- Metadata Footer: Toont uitvoeringstijd en aantal resultaten onder de tabel.
- Multi-Tab Support: Open meerdere bestanden of query's tegelijk.
- File Filtering: DirectoryTree toont alleen ondersteunde bestanden.
"""

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, Middle
from textual.widgets import (
    Header, Footer, TextArea, DataTable, DirectoryTree, 
    Static, Input, Button, TabbedContent, TabPane
)
from textual.screen import ModalScreen
from pathlib import Path
from datetime import datetime
from typing import Iterable, List, Any, Optional
import duckdb
import os
import asyncio
import time

# =====================
# Config & Helpers
# =====================
CWD = Path(os.getcwd())
HISTORY_FILE = CWD / "duckcli_history.sql"
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

def append_history(sql: str):
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(f"\n-- ----------------------------------------\n-- {datetime.now().isoformat()}\n{sql.strip()}\n")

# =====================
# UI Components
# =====================
class AboutScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        with Middle():
            with Vertical(id="about-inner"):
                yield Static("[b]OpenDuck CLI[/b]", id="about-title")
                yield Static("----------------------------------------")
                yield Static("[b]GitHub:[/b] github.com/bertatron/duckcli")
                yield Static("[b]Author:[/b] Bertatron")
                yield Static("[b]License:[/b] MIT")
                yield Static("\nKISS - Metadata & Async enabled.")
                yield Static("\n[Press any key to close]", id="about-footer")
    def on_key(self) -> None: self.dismiss()
    def on_click(self) -> None: self.dismiss()

class DuckTree(DirectoryTree):
    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir() or is_duckdb_file(p)]

class HeaderFilterMenu(ModalScreen):
    def __init__(self, col_name: str, current_filter: str, current_sort: Optional[str]):
        super().__init__()
        self.col_name, self.current_filter, self.current_sort = col_name, current_filter, current_sort

    def compose(self) -> ComposeResult:
        with Vertical(id="menu"):
            yield Static(f"Column: {self.col_name}", id="menu-title")
            yield Input(value=self.current_filter, placeholder="Filter text...", id="filter-input")
            with Horizontal():
                yield Button("ASC ▲", id="btn-asc")
                yield Button("DESC ▼", id="btn-desc")
                yield Button("Reset", variant="error", id="btn-clear-sort")
            with Horizontal():
                yield Button("Apply", variant="primary", id="btn-apply")
                yield Button("Cancel", id="btn-cancel")

    def on_mount(self): self.query_one("#filter-input").focus()

    def on_button_pressed(self, event: Button.Pressed):
        f_val = self.query_one("#filter-input", Input).value
        if event.button.id == "btn-cancel": self.dismiss()
        elif event.button.id == "btn-apply": self.dismiss({"filter": f_val, "sort": self.current_sort})
        elif event.button.id == "btn-asc": self.dismiss({"filter": f_val, "sort": "asc"})
        elif event.button.id == "btn-desc": self.dismiss({"filter": f_val, "sort": "desc"})
        elif event.button.id == "btn-clear-sort": self.dismiss({"filter": f_val, "sort": None})

class QueryTab(Vertical):
    def __init__(self, sql: str = ""):
        super().__init__()
        self.initial_sql = sql
        self.full_data, self.column_names, self.col_states = [], [], {}

    def compose(self) -> ComposeResult:
        yield TextArea(self.initial_sql, language="sql", id="query-input")
        yield DataTable(id="results-table")
        yield Static("Ready", id="metadata-bar")

    def on_mount(self):
        t = self.query_one(DataTable)
        t.cursor_type, t.zebra_stripes = "row", True

class DuckCLI(App):
    CSS = """
    Screen { layout: horizontal; }
    DuckTree { width: 30%; border-right: tall $primary; background: $surface; }
    TabbedContent { width: 70%; }
    TextArea { height: 35%; border-bottom: tall $primary; }
    DataTable { height: 60%; }
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
    #about-inner { width: 60; height: auto; background: $surface; border: tall $primary; padding: 2 4; }
    #about-title { text-align: center; color: $accent; }
    #about-footer { text-align: center; color: $text-disabled; }
    """
    BINDINGS = [
        ("ctrl+enter", "run_query", "Run"), 
        ("ctrl+w", "close_tab", "Close Tab"), 
        ("ctrl+a", "about", "About"),
        ("ctrl+q", "quit", "Quit")
    ]

    def __init__(self):
        super().__init__()
        self.con = duckdb.connect()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Horizontal(DuckTree(CWD, id="files"), TabbedContent(id="tabs"))
        yield Footer()

    def on_mount(self):
        sql = ""
        if HISTORY_FILE.exists():
            text = HISTORY_FILE.read_text(encoding="utf-8")
            if "-- ----------------------------------------" in text:
                sql = text.strip().split("-- ----------------------------------------")[-1].strip()
        self.run_worker(self.add_new_tab("Main", sql))

    async def add_new_tab(self, name: str, sql: str, run: bool = False):
        tabs = self.query_one("#tabs", TabbedContent)
        tab_id = f"t{int(datetime.now().timestamp() * 1000)}"
        await tabs.add_pane(TabPane(name, QueryTab(sql), id=tab_id))
        tabs.active = tab_id
        if run: self.set_timer(0.1, self.action_run_query)

    async def action_run_query(self):
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active: return
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        sql = tab.query_one(TextArea).text.strip()
        if not sql: return
        
        append_history(sql)
        tbl = tab.query_one(DataTable)
        meta = tab.query_one("#metadata-bar")
        tbl.loading = True
        meta.update("Executing...")

        def execute():
            start = time.perf_counter()
            cursor = self.con.cursor()
            res = cursor.execute(sql)
            if not res.description:
                duration = time.perf_counter() - start
                return [], ["Info"], [["Success"]], duration
            
            cols = [d[0] for d in res.description]
            data = [list(r) for r in res.fetchall()]
            duration = time.perf_counter() - start
            return data, cols, None, duration

        try:
            data, cols, _, duration = await asyncio.to_thread(execute)
            tab.column_names = cols
            tab.full_data = data
            tab.col_states = {i: {"filter": "", "sort": None} for i in range(len(cols))}
            self.refresh_tab_table(tab)
            meta.update(f"Rows: {len(data)} | Time: {duration:.4f}s | Finished: {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            tbl.clear(columns=True)
            tbl.add_column("Error")
            tbl.add_row(str(e))
            meta.update("Error occurred")
        finally:
            tbl.loading = False

    def refresh_tab_table(self, tab: QueryTab):
        tbl, data = tab.query_one(DataTable), [r[:] for r in tab.full_data]
        for i, s in tab.col_states.items():
            if s["filter"]:
                f = s["filter"].lower()
                data = [r for r in data if f in str(r[i]).lower()]
        for i, s in tab.col_states.items():
            if s["sort"]:
                data.sort(key=lambda x: x[i] if x[i] is not None else "", reverse=(s["sort"]=="desc"))
                break
        
        tbl.clear(columns=True)
        for i, name in enumerate(tab.column_names):
            s = tab.col_states[i]
            lbl = f"{name}{' ▲' if s['sort']=='asc' else ' ▼' if s['sort']=='desc' else ''}{' 🔍' if s['filter'] else ''}"
            tbl.add_column(lbl, key=str(i))
        for r in data: tbl.add_row(*[str(v) if v is not None else "NULL" for v in r])

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected):
        tabs = self.query_one("#tabs", TabbedContent)
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        idx = int(event.column_key.value)
        def handle(res):
            if res:
                for i in tab.col_states: tab.col_states[i]["sort"] = None
                tab.col_states[idx].update(res); self.refresh_tab_table(tab)
        self.push_screen(HeaderFilterMenu(tab.column_names[idx], tab.col_states[idx]["filter"], tab.col_states[idx]["sort"]), handle)

    async def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        if is_duckdb_file(Path(event.path)):
            await self.add_new_tab(Path(event.path).name, sql_for_file(Path(event.path)), run=True)

    def action_about(self): self.push_screen(AboutScreen())
    def action_close_tab(self):
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active: tabs.remove_pane(tabs.active)
    def action_quit(self): self.exit()

if __name__ == "__main__":
    DuckCLI().run()