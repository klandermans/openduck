"""
DuckCLI Features:
- Async Execution: Query's draaien in een aparte thread; interface hangt niet.
- Metadata Footer: Toont uitvoeringstijd en aantal resultaten onder de tabel.
- Multi-Tab Support: Open meerdere bestanden of query's tegelijk.
- File Filtering: DirectoryTree toont alleen ondersteunde bestanden.
- Save Queries: Save queries with names to openduck.json
- History: Store query history in openduck.json
"""

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, Middle, ScrollableContainer
from textual.widgets import (
    Header, Footer, TextArea, DataTable, DirectoryTree,
    Static, Input, Button, TabbedContent, TabPane, ListView, ListItem, Label
)
from textual.events import MouseEvent
from textual.screen import ModalScreen
from pathlib import Path
from datetime import datetime
from typing import Iterable, List, Any, Optional
import duckdb
import os
import asyncio
import time
import json

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
            return json.load(f)
    else:
        # Create default config
        default_config = {
            "history": [],
            "saved_queries": []
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
        with Horizontal():  # Container for table and export sidebar
            yield DataTable(id="results-table")
            with Vertical(id="export-sidebar"):
                yield Button("↓CSV", id="export-csv", classes="export-btn")
                yield Button("↓XLSX", id="export-excel", classes="export-btn")
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

class DuckCLI(App):
    CSS = """
    Screen { layout: horizontal; }
    #sidebar { width: 30%; border-right: tall $primary; background: $surface; }
    #main-content { width: 70%; }
    #files-container { height: 1fr; }
    #history-container { height: 1fr; }
    #saved-queries-container { height: 1fr; }
    #history-list, #saved-queries-list { height: 1fr; }
    TabbedContent { width: 100%; }
    TextArea { height: 35%; border-bottom: tall $primary; }
    DataTable { height: 60%; }
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
    #about-inner { width: 60; height: auto; background: $surface; border: tall $primary; padding: 2 4; }
    #about-title { text-align: center; color: $accent; }
    #about-footer { text-align: center; color: $text-disabled; }
    #save-dialog { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
    #dialog-title { text-style: bold; margin-bottom: 1; text-align: center; }
    #query-preview { height: 3; border: solid $primary; padding: 1; }
    #export-modal { width: 50; height: auto; background: $surface; border: tall $primary; padding: 2; }
    #export-modal-title { text-style: bold; margin-bottom: 1; text-align: center; }
    #export-filename { margin-bottom: 1; }
    """
    BINDINGS = [
        ("ctrl+enter", "run_query", "Run"),
        ("ctrl+s", "save_query", "Save Query"),
        ("ctrl+w", "close_tab", "Close Tab"),
        ("ctrl+a", "about", "About"),
        ("ctrl+q", "quit", "Quit")
    ]

    def __init__(self):
        super().__init__()
        self.con = duckdb.connect()
        self.config = load_config()

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Static("Explorer", id="explorer-header")
                yield DuckTree(CWD, id="files")
                yield Static("Saved Queries", id="saved-header")
                yield ListView(id="saved-queries-list")
                yield Static("History", id="history-header")
                yield ListView(id="history-list")
            with Vertical(id="main-content"):
                yield TabbedContent(id="tabs")
        yield Header()
        yield Footer()

    def on_mount(self):
        # Load history and saved queries
        self.load_history_list()
        self.load_saved_queries_list()
        
        # Load last query from history if available
        sql = ""
        if self.config["history"]:
            sql = self.config["history"][-1]["sql"]
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

        # Update config after adding to history
        add_to_history(sql)
        self.config = load_config()  # Reload config to ensure it's up to date
        self.load_history_list()  # Refresh the history list in UI
        
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

    def on_list_view_selected(self, event: ListView.Selected):
        """Handle selection from history or saved queries"""
        item_id = event.item.id
        
        if item_id.startswith("hist_"):
            # Extract the timestamp from the ID (last part after the second underscore)
            parts = item_id.split('_', 2)  # Split into 3 parts: ['hist', 'index', 'timestamp_us']
            if len(parts) == 3 and parts[2].isdigit():
                timestamp_us = int(parts[2])
                
                # Find the history item with the closest timestamp
                for hist_item in self.config["history"]:
                    item_timestamp = int(datetime.fromisoformat(hist_item["timestamp"]).timestamp() * 1000000)
                    if item_timestamp == timestamp_us:
                        self.load_query_in_current_tab(hist_item["sql"])
                        break
        elif item_id.startswith("saved_"):
            # Extract the name from the ID (before the last underscore and index)
            parts = item_id.rsplit('_', 1)  # Split from the right, keeping the index part
            if len(parts) == 2 and parts[1].isdigit():
                name = parts[0].replace("saved_", "")
                for saved_item in self.config["saved_queries"]:
                    if saved_item["name"] == name:
                        self.load_query_in_current_tab(saved_item["sql"])
                        break
    
    def load_query_in_current_tab(self, sql: str):
        """Load a query into the current active tab"""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active: return
        
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        textarea = tab.query_one(TextArea)
        textarea.text = sql

    def load_history_list(self):
        """Load history into the history list view"""
        history_list = self.query_one("#history-list", ListView)
        history_list.clear()
        
        # Show all history items (unlimited) - reversed to show most recent first
        for i, item in enumerate(reversed(self.config["history"])):
            timestamp = datetime.fromisoformat(item["timestamp"]).strftime("%H:%M:%S")
            label = f"[{timestamp}] {item['sql'][:50]}{'...' if len(item['sql']) > 50 else ''}"
            # Create a safe ID by using a prefix and the index to ensure uniqueness
            safe_id = f"hist_{i}_{int(datetime.fromisoformat(item['timestamp']).timestamp()*1000000)}"
            list_item = ListItem(Label(label), id=safe_id)
            try:
                history_list.append(list_item)
            except Exception:
                # If there's an issue with appending, try with a more unique ID
                safe_id = f"hist_{i}_{int(datetime.fromisoformat(item['timestamp']).timestamp()*1000000)}_{hash(item['sql']) % 10000}"
                list_item = ListItem(Label(label), id=safe_id)
                history_list.append(list_item)

    def load_saved_queries_list(self):
        """Load saved queries into the saved queries list view"""
        saved_list = self.query_one("#saved-queries-list", ListView)
        saved_list.clear()
        
        for i, item in enumerate(self.config["saved_queries"]):
            timestamp = datetime.fromisoformat(item["timestamp"]).strftime("%d/%m %H:%M")
            label = f"{item['name']} [{timestamp}]"
            # Create a safe ID for the saved query using name and index to ensure uniqueness
            safe_name = item['name'].replace(' ', '_').replace('-', '_')
            list_item = ListItem(Label(label), id=f"saved_{safe_name}_{i}")
            try:
                saved_list.append(list_item)
            except Exception:
                # If there's an issue with appending, try with a more unique ID
                safe_id = f"saved_{safe_name}_{i}_{int(datetime.fromisoformat(item['timestamp']).timestamp())}"
                list_item = ListItem(Label(label), id=safe_id)
                saved_list.append(list_item)

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

    def on_button_pressed(self, event: Button.Pressed):
        """Handle export button presses"""
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

if __name__ == "__main__":
    DuckCLI().run()