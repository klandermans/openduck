"""Main OpenDuck application class."""

import asyncio
import base64
from datetime import datetime
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Optional

try:
    import resource
except ImportError:
    resource = None

import duckdb
from textual.app import App, ComposeResult
from textual.content import Content
from textual.containers import Horizontal, Vertical
from textual.events import Click
from textual.widgets import (
    Button, DataTable, DirectoryTree, Footer, Header, Input,
    TabbedContent, TabPane, Tree, TextArea
)
from textual.widgets._tabbed_content import ContentTab, ContentTabs
from textual.widgets.text_area import Selection

from openduck.core.clipboard import copy_to_system_clipboard, get_system_clipboard
from openduck.core.config import (
    CONFIG_FILE,
    CWD,
    MAX_UI_ROWS,
    add_to_history,
    delete_connection,
    load_config,
    save_connection,
)
from openduck.core.error_classifier import classify_file_error
from openduck.core.sql_utils import is_duckdb_file, sql_for_file
from openduck.gui.dialogs.add_connection_dialog import AddConnectionDialog
from openduck.gui.dialogs.export_dialog import ExportDialog
from openduck.gui.dialogs.plot_dialog import PlotDialog
from openduck.gui.dialogs.save_query_dialog import SaveQueryDialog
from openduck.gui.screens.about import AboutScreen
from openduck.gui.styles import APP_CSS
from openduck.gui.widgets.database_tree import DatabaseTree
from openduck.gui.widgets.file_tree import DuckTree
from openduck.gui.widgets.query_tab import QueryTab
from openduck.gui.widgets.resize_handle import ResizeHandle
from openduck.gui.widgets.results_table import ResultsTable
from openduck.gui.widgets.sql_editor import SqlTextArea

logger = logging.getLogger(__name__)


class DuckCLI(App):
    """OpenDuck - A flat file SQL client based on DuckDB."""

    TITLE = "OpenDuck"
    SUB_TITLE = "done"
    ENABLE_MOUSE_SUPPORT = True
    ALLOW_SELECT = False
    CSS = APP_CSS

    BINDINGS = [
        ("ctrl+enter,super+enter,cmd+enter,command+enter", "run_query", "Run"),
        ("ctrl+s,super+s,cmd+s,command+s", "save_query", "Save Query"),
        ("ctrl+t,super+t,cmd+t,command+t", "new_tab", "New Tab"),
        ("f3,ctrl+slash,super+slash,cmd+slash,command+slash,ctrl+underscore,super+underscore,cmd+underscore,command+underscore,ctrl+7,super+7,ctrl+minus,super+minus", "toggle_comment", "Comment (F3)"),
        ("ctrl+z,super+z,cmd+z,command+z", "undo", "Undo"),
        ("ctrl+y,super+y,cmd+y,command+y,shift+ctrl+z,ctrl+shift+z,shift+super+z,super+shift+z,shift+cmd+z,cmd+shift+z,shift+command+z,command+shift+z", "redo", "Redo"),
        ("ctrl+w,super+w,cmd+w,command+w", "close_tab", "Close Tab"),
        ("ctrl+m", "toggle_mouse", "Toggle Mouse"),
        ("f1", "about", "About"),
        ("ctrl+q,super+q,cmd+q,command+q", "quit", "Quit")
    ]

    def __init__(self):
        super().__init__()
        self.con = duckdb.connect()
        self.config = load_config()
        self.db_connections: dict = {}
        self._tab_base_names: dict[str, str] = {}
        self._tab_counter: int = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Horizontal():
            with Vertical(id="sidebar"):
                yield DuckTree(CWD, id="files")
                # yield DatabaseTree()  # Temporarily disabled
                yield Tree("Saved Queries", id="saved-queries-tree")
                yield Tree("History", id="history-tree")
            yield ResizeHandle()
            with Vertical(id="main-content"):
                yield TabbedContent(id="tabs")
        yield Footer()

    def format_title(self, title: Optional[str] = None, sub_title: Optional[str] = None) -> Content:
        """Format the title and subtitle for display in the header.

        Line 1: Bold title followed by status dot with status color.
        Line 2: Subtitle styled with status color.
        """
        title_str = title if title else (getattr(self, "TITLE", "OpenDuck") or "OpenDuck")
        sub_str = sub_title if sub_title is not None else (getattr(self, "SUB_TITLE", "done") or "")

        clean_sub = sub_str.strip()
        prefixes = ("• ", "•", "● ", "●", "🟢 ", "🟢", "🔴 ", "🔴", "🟡 ", "🟡")
        changed = True
        while changed:
            changed = False
            for prefix in prefixes:
                if clean_sub.startswith(prefix):
                    clean_sub = clean_sub[len(prefix):].strip()
                    changed = True
                    break

        lower = clean_sub.lower()
        if "running" in lower or "exec" in lower:
            color = "red"
        elif "error" in lower or "fail" in lower:
            color = "red"
        elif "cancel" in lower:
            color = "yellow"
        else:
            color = "green"

        parts = [
            (f"{title_str} ", "bold"),
            ("•", color),
        ]
        if clean_sub:
            parts.append((f"\n{lower}", color))

        return Content.assemble(*parts)


    def _emit_terminal_seq(self, seq: str) -> None:
        """Emit terminal escape sequence via driver and directly to stdout/stderr and /dev/tty."""
        driver = getattr(self, "_driver", None)
        if driver is not None and hasattr(driver, "write"):
            try:
                driver.write(seq)
                if hasattr(driver, "flush"):
                    driver.flush()
            except Exception:
                pass
        for stream in (sys.__stdout__, sys.__stderr__):
            try:
                if stream and hasattr(stream, "write"):
                    stream.write(seq)
                    if hasattr(stream, "flush"):
                        stream.flush()
            except Exception:
                pass
        try:
            with open("/dev/tty", "w") as tty:
                tty.write(seq)
                tty.flush()
        except Exception:
            pass

    def _set_terminal_title(self, title: str) -> None:
        """Set terminal tab and window title using OSC 0, OSC 1 and OSC 2 escape sequences."""
        seq = f"\033]0;{title}\007\033]1;{title}\007\033]2;{title}\007"
        self._emit_terminal_seq(seq)
        if (
            sys.platform == "darwin"
            and os.environ.get("TERM_PROGRAM") == "iTerm.app"
            and getattr(sys.stdout, "isatty", lambda: False)()
            and "unittest" not in sys.modules
        ):
            try:
                tty_path = None
                try:
                    tty_path = os.ttyname(sys.stdout.fileno())
                except Exception:
                    pass
                escaped_title = title.replace('"', '\\"')
                if tty_path:
                    script = (
                        f'tell application "iTerm"\n'
                        f'  repeat with w in windows\n'
                        f'    repeat with t in tabs of w\n'
                        f'      repeat with s in sessions of t\n'
                        f'        if (tty of s) is "{tty_path}" then\n'
                        f'          tell s to set name to "{escaped_title}"\n'
                        f'          return\n'
                        f'        end if\n'
                        f'      end repeat\n'
                        f'    end repeat\n'
                        f'  end repeat\n'
                        f'end tell'
                    )
                else:
                    script = f'tell application "iTerm" to tell current session of current window to set name to "{escaped_title}"'
                subprocess.Popen(["osascript", "-e", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

    def _push_terminal_title(self) -> None:
        """Save terminal window/tab title to stack."""
        self._emit_terminal_seq("\033[22;0t")

    def _pop_terminal_title(self) -> None:
        """Restore previous terminal window/tab title from stack."""
        self._emit_terminal_seq("\033[23;0t")

    def is_any_query_running(
        self,
        exclude_task: Optional[asyncio.Task] = None,
        exclude_pane_id: Optional[str] = None,
    ) -> bool:
        """Check if any query tab currently has an active running query task."""
        if exclude_task is None:
            try:
                exclude_task = asyncio.current_task()
            except Exception:
                exclude_task = None
        try:
            tabs = self.query_one("#tabs", TabbedContent)
            for pane in tabs.query(TabPane):
                if exclude_pane_id and pane.id == exclude_pane_id:
                    continue
                try:
                    qtab = pane.query_one(QueryTab)
                    task = getattr(qtab, "running_task", None)
                    if task and not task.done():
                        if hasattr(task, "cancelling") and task.cancelling():
                            continue
                        if exclude_task is None or task is not exclude_task:
                            return True
                except Exception:
                    pass
        except Exception:
            pass
        return False

    def _close_tab_by_id(self, pane_id: str) -> None:
        """Helper to cancel any running task on a tab before closing it, and refresh status."""
        try:
            tab = self.query_one(f"#{pane_id}").query_one(QueryTab)
            if hasattr(tab, "running_task") and tab.running_task:
                if not tab.running_task.done():
                    tab.running_task.cancel()
                tab.running_task = None
        except Exception:
            pass

        self._tab_base_names.pop(pane_id, None)

        try:
            tabs = self.query_one("#tabs", TabbedContent)
            tabs.remove_pane(pane_id)
        except Exception:
            pass

        # If no queries are running anywhere, ensure status is done
        if not self.is_any_query_running(exclude_pane_id=pane_id):
            self.set_status("done")

    def _clean_base_name(self, raw: str) -> str:
        """Extract clean base name from tab label, removing prefixes, status and close button."""
        s = (raw or "").strip()
        if s.endswith("✕"):
            s = s[:-1].strip()
        if "•" in s:
            s = s.split("•")[0].strip()
        if "●" in s:
            s = s.split("●")[0].strip()
        for prefix in ("❌ ", "❌", "🟢 ", "🟢", "🔴 ", "🔴", "🟡 ", "🟡"):
            if s.startswith(prefix):
                s = s[len(prefix):].strip()
        return s or "Main"

    def _update_tab_label(self, pane_id: str, status: str, icon: Optional[str] = None) -> None:
        """Update a specific tab's ContentTab label with status indicator."""
        try:
            tabs = self.query_one("#tabs", TabbedContent)
            tab_widget = tabs.get_tab(pane_id)
            if tab_widget is None:
                return

            base_name = self._tab_base_names.get(pane_id)
            if not base_name:
                raw_label = getattr(tab_widget, "label_text", str(getattr(tab_widget, "label", "")))
                base_name = self._clean_base_name(raw_label)
                self._tab_base_names[pane_id] = base_name

            clean_status = (status or "").strip()
            prefixes = ("• ", "•", "● ", "●", "🟢 ", "🟢", "🔴 ", "🔴", "🟡 ", "🟡")
            changed = True
            while changed:
                changed = False
                for prefix in prefixes:
                    if clean_status.startswith(prefix):
                        clean_status = clean_status[len(prefix):].strip()
                        changed = True
                        break

            status_lower = clean_status.lower()
            if any(w in status_lower for w in ("done", "ready", "success")):
                status_color = "green"
            elif any(w in status_lower for w in ("running", "executing", "error", "failed")):
                status_color = "red"
            elif any(w in status_lower for w in ("cancelled", "cancel", "warn")):
                status_color = "yellow"
            else:
                status_color = "white"

            dot = icon if icon is not None else "•"

            if clean_status:
                new_label = Content.from_markup(
                    f"{base_name} [{status_color}]{dot} {clean_status}[/{status_color}] ✕"
                )
            else:
                new_label = Content.from_markup(f"{base_name} ✕")

            tab_widget.label = new_label
            try:
                pane = self.query_one(f"#{pane_id}", TabPane)
                pane._title = new_label
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Error updating tab label for {pane_id}: {e}")

    def set_status(
        self,
        status: str,
        icon: Optional[str] = None,
        details: Optional[str] = None,
        pane_id: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        """Update execution status in terminal tab/window title and active ContentTab.

        Args:
            status: Status name, e.g. 'running', 'done', 'ready', 'error', 'cancelled'.
            icon: Optional icon override. Defaults to bullet '•'.
            details: Optional extra text (e.g. elapsed time).
            pane_id: Optional tab pane ID to update. If None, the active tab is updated.
        """
        try:
            clean_status = (status or "").strip()
            prefixes = ("• ", "•", "● ", "●", "🟢 ", "🟢", "🔴 ", "🔴", "🟡 ", "🟡")
            changed = True
            while changed:
                changed = False
                for prefix in prefixes:
                    if clean_status.startswith(prefix):
                        clean_status = clean_status[len(prefix):].strip()
                        changed = True
                        break

            label = f"{clean_status} ({details})" if details else clean_status
            self.sub_title = label

            status_lower = clean_status.lower()
            if any(w in status_lower for w in ("running", "executing")):
                default_icon = "🔴"
            elif any(w in status_lower for w in ("cancel", "cancelled")):
                default_icon = "🟡"
            elif any(w in status_lower for w in ("error", "failed")):
                default_icon = "🔴"
            else:
                default_icon = "🟢"

            dot = icon if icon is not None else default_icon
            term_title = f"{self.TITLE} {dot} {label}".strip() if label else f"{self.TITLE} {dot}"
            self._set_terminal_title(term_title)

            # iTerm2 OSC 133 shell integration and OSC 1337 user variables
            osc_seqs = []
            status_lower = clean_status.lower()
            if any(w in status_lower for w in ("running", "executing")):
                osc_seqs.append("\033]133;C\007")
            elif any(w in status_lower for w in ("cancel", "cancelled")):
                osc_seqs.append("\033]133;D;130\007")
            elif any(w in status_lower for w in ("error", "failed")):
                osc_seqs.append("\033]133;D;1\007")
            else:
                osc_seqs.append("\033]133;D;0\007")

            try:
                b64_status = base64.b64encode(clean_status.encode("utf-8")).decode("ascii")
                osc_seqs.append(f"\033]1337;SetUserVar=status={b64_status}\007")
            except Exception:
                pass

            if osc_seqs:
                self._emit_terminal_seq("".join(osc_seqs))

            # Update ContentTab label
            try:
                tabs = self.query_one("#tabs", TabbedContent)
                target_pane_id = pane_id or tabs.active
                if target_pane_id:
                    self._update_tab_label(target_pane_id, clean_status, icon=icon)
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Error updating status: {e}")

    def on_mount(self):
        # Push current terminal title so we can restore it on exit
        self._push_terminal_title()
        self.set_status("done")

        # Let Textual initialize its standard mouse driver cleanly without raw ANSI overrides
        self.load_history_list()
        self.load_saved_queries_list()
        # self.load_saved_connections()  # Temporarily disabled
        self.set_interval(1.0, self.update_memory_info)

        sql = ""
        if self.config["history"]:
            sql = self.config["history"][-1]["sql"]
        # Delay adding the first tab slightly to allow UI to settle
        self.set_timer(0.2, lambda: self.run_worker(self.add_new_tab("Main", sql)))

    def on_unmount(self):
        self._pop_terminal_title()

    def _get_process_memory_mb(self) -> Optional[float]:
        """Return current process memory usage in MB across macOS, Linux, and Windows."""
        if resource is not None:
            mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform == "darwin":
                return mem / (1024 * 1024)
            return mem / 1024
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            pmc = PROCESS_MEMORY_COUNTERS()
            pmc.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                return pmc.WorkingSetSize / (1024 * 1024)
        return None

    def update_memory_info(self):
        """Update the memory info in the active tab's footer bar."""
        try:
            mem_mb = self._get_process_memory_mb()
            if mem_mb is None:
                return

            tabs = self.query_one("#tabs", TabbedContent)
            if tabs.active:
                tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
                mem_bar = tab.query_one("#memory-bar")
                color = "red" if mem_mb > 2048 else "cyan"
                mem_bar.update(f"Memory: [{color}]{mem_mb:.1f} MB[/{color}]")
        except Exception:
            pass

    def load_saved_connections(self):
        """Load saved connections from config and attempt to connect."""
        for conn_info in self.config.get("connections", []):
            self.run_worker(self.connect_database(conn_info))

    async def connect_database(self, conn_info: dict):
        """Establish a database connection and populate tree."""
        conn_id = conn_info["id"]
        db_tree = self.query_one("#db-tree", DatabaseTree)
        db_tree.add_connection_node(conn_info)
        self.set_status("running")
        try:
            if conn_info["type"] == "mysql":
                tables = await self._connect_mysql(conn_info)
            elif conn_info["type"] == "mssql":
                tables = await self._connect_mssql(conn_info)
            else:
                raise ValueError(f"Unknown type: {conn_info['type']}")
            db_tree.update_tables(conn_id, tables)
            self.set_status("done")
        except Exception as e:
            db_tree.remove_connection_node(conn_id)
            err_node = db_tree.original_tree.root.add(
                f"[red]ERR: {conn_info['display_name']}[/red]",
                data={"type": "error", "conn_id": conn_id},
            )
            err_node.add_leaf(f"[red]{str(e)[:60]}[/red]")
            self.db_connections.pop(conn_id, None)
            self.set_status("error")

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
            node = db_tree.original_tree.cursor_node
            if node and node.data:
                conn_id = node.data.get("conn_id")
                if conn_id:
                    self._disconnect(conn_id)
        except Exception:
            pass

    def _is_tab_empty_and_unsaved(self, pane_id: str) -> bool:
        """Check if a tab has no query content, no results, and is an unsaved placeholder."""
        try:
            pane = self.query_one(f"#{pane_id}", TabPane)
            qtab = pane.query_one(QueryTab)
            # Check running task
            task = getattr(qtab, "running_task", None)
            if task and not task.done():
                return False
            # Check editor content
            try:
                editor = qtab.query_one(SqlTextArea)
            except Exception:
                editor = qtab.query_one(TextArea)
            if editor.text.strip():
                return False
            # Check query results
            if getattr(qtab, "full_data", None) and len(qtab.full_data) > 0:
                return False
            # Check if saved
            base_name = self._tab_base_names.get(pane_id, "")
            saved_names = {q.get("name") for q in self.config.get("saved_queries", []) if isinstance(q, dict)}
            if base_name in saved_names:
                return False
            return base_name in ("Main", "New Tab", "Untitled", "")
        except Exception:
            return False

    async def add_new_tab(self, name: str, sql: str, run: bool = False, connection_id: str = None):
        tabs = self.query_one("#tabs", TabbedContent)
        self._tab_counter += 1
        tab_id = f"t{int(datetime.now().timestamp() * 1000)}_{self._tab_counter}"
        clean_name = self._clean_base_name(name)
        self._tab_base_names[tab_id] = clean_name
        initial_status = "running" if run else "done"
        color = "red" if run else "green"
        pane_label = Content.from_markup(f"{clean_name} [{color}]• {initial_status}[/{color}] ✕")
        await tabs.add_pane(TabPane(pane_label, QueryTab(sql, connection_id=connection_id), id=tab_id))
        tabs.active = tab_id
        self.set_status(initial_status, pane_id=tab_id)
        if run:
            self.set_timer(0.1, lambda: self.run_worker(self.action_run_query(pane_id=tab_id)))

    def _classify_file_error(self, e: Exception, file_path: str = "") -> str:
        """Classify file-related errors and return user-friendly message."""
        return classify_file_error(e, file_path)

    async def _load_and_register_excel(self, file_path: Path) -> str:
        """Load Excel file using pandas in a worker thread and register as DuckDB table."""
        stem = file_path.stem.replace(' ', '_').replace('-', '_')
        table_name = re.sub(r'\W|^(?=\d)', '_', stem)

        def do_register():
            import pandas as pd
            df = pd.read_excel(file_path)
            self.con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df')
            logger.debug(f"Registered Excel file '{file_path}' as table '{table_name}' ({len(df)} rows)")

        await asyncio.to_thread(do_register)
        return table_name

    async def action_run_query(self, pane_id: Optional[str] = None):
        tabs = self.query_one("#tabs", TabbedContent)
        tab_pane_id = pane_id or tabs.active
        if not tab_pane_id:
            logger.debug("No active tab found")
            return
        tab = self.query_one(f"#{tab_pane_id}").query_one(QueryTab)
        editor = tab.query_one(SqlTextArea)

        # Execute selected text if any is selected (like VS Code / DBeaver)
        selected_sql = editor.selected_text.strip()
        sql = selected_sql if selected_sql else editor.text.strip()
        if not sql:
            logger.debug("No SQL query to execute")
            self.notify("Geen SQL query om uit te voeren", timeout=1.5)
            return

        non_comment_lines = []
        for line in sql.splitlines():
            s = line.strip()
            if s and not s.startswith("--"):
                non_comment_lines.append(s)
        cleaned_sql = " ".join(non_comment_lines).strip()
        cleaned_sql = re.sub(r'/\*.*?\*/', '', cleaned_sql, flags=re.DOTALL).strip()
        if not cleaned_sql:
            self.notify("Query is uitgecommentarieerd (--), niet uitgevoerd", timeout=2.0)
            meta = tab.query_one("#metadata-bar")
            meta.update("Overgeslagen (alleen commentaar)")
            return

        if selected_sql:
            self.notify("Geselecteerde query uitvoeren...", timeout=1.0)

        logger.debug(f"Executing query: {sql[:100]}{'...' if len(sql) > 100 else ''}")

        # Update config after adding to history
        add_to_history(sql)
        self.config = load_config()  # Reload config to ensure it's up to date
        self.load_history_list()  # Refresh the history list in UI

        # Cancel any previous running task on this tab before starting a new one
        if getattr(tab, "running_task", None) and not tab.running_task.done():
            try:
                tab.running_task.cancel()
            except Exception:
                pass

        current_task = asyncio.current_task()
        tab.running_task = current_task
        tbl = tab.query_one(DataTable)
        meta = tab.query_one("#metadata-bar")
        tbl.loading = True
        meta.update("Executing...")
        tab.last_status = "running"
        self._update_tab_label(tab_pane_id, "running")
        if tabs.active == tab_pane_id:
            self.set_status("running", pane_id=tab_pane_id)
        logger.debug("Query execution started, task tracked")

        conn_id = tab.connection_id
        conn_meta = self.db_connections.get(conn_id) if conn_id else None
        logger.debug(f"Connection info: {conn_meta['type'] if conn_meta else 'None'}")

        def execute():
            start = time.perf_counter()
            logger.debug("Executing SQL query in thread")

            if conn_meta and conn_meta["type"] == "mssql":
                mssql_conn = conn_meta["connection"]
                cursor = mssql_conn.cursor()
                cursor.execute(sql)
                if not cursor.description:
                    logger.debug("Query returned no results (MS SQL)")
                    return [], ["Info"], None, time.perf_counter() - start, False
                cols = [d[0] for d in cursor.description]
                raw_data = cursor.fetchmany(MAX_UI_ROWS + 1)
                is_truncated = len(raw_data) > MAX_UI_ROWS
                data = [list(r) for r in raw_data[:MAX_UI_ROWS]]
                logger.debug(f"MS SQL query executed: {len(data)} rows (truncated={is_truncated}), {len(cols)} columns")
                return data, cols, None, time.perf_counter() - start, is_truncated
            else:
                cursor = self.con.cursor()
                res = cursor.execute(sql)
                if not res.description:
                    duration = time.perf_counter() - start
                    logger.debug("Query returned no results (DuckDB)")
                    return [], ["Info"], [["Success"]], duration, False
                cols = [d[0] for d in res.description]
                raw_data = res.fetchmany(MAX_UI_ROWS + 1)
                is_truncated = len(raw_data) > MAX_UI_ROWS
                data = [list(r) for r in raw_data[:MAX_UI_ROWS]]
                duration = time.perf_counter() - start
                logger.debug(f"DuckDB query executed: {len(data)} rows (truncated={is_truncated}), {len(cols)} columns")
                return data, cols, None, duration, is_truncated

        try:
            data, cols, _, duration, is_truncated = await asyncio.to_thread(execute)
            if tab.running_task is not current_task:
                return
            tab.column_names = cols
            tab.full_data = data
            tab.col_states = {i: {"filter": "", "sort": None} for i in range(len(cols))}
            logger.debug(f"Setting up table with {len(cols)} columns and {len(data)} rows")
            self.refresh_tab_table(tab)
            conn_label = ""
            if conn_meta:
                conn_label = f" | via {conn_meta['type'].upper()}"
            rows_label = f"{MAX_UI_ROWS}+ (truncated)" if is_truncated else str(len(data))
            meta.update(f"Rows: {rows_label} | Time: {duration:.4f}s{conn_label} | Finished: {datetime.now().strftime('%H:%M:%S')}")
            logger.debug(f"Query completed successfully: {len(data)} rows in {duration:.4f}s")
            tab.last_status = "done"
            self._update_tab_label(tab_pane_id, "done")
            if not self.is_any_query_running(exclude_task=current_task):
                self.set_status("done")
        except (asyncio.CancelledError, getattr(duckdb, "InterruptException", ())):
            logger.debug("Query was cancelled")
            if tab.running_task is current_task:
                meta.update("Query cancelled")
                tab.last_status = "cancelled"
                self._update_tab_label(tab_pane_id, "cancelled")
                if not self.is_any_query_running(exclude_task=current_task):
                    self.set_status("cancelled")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            if tab.running_task is current_task:
                tbl.clear(columns=True)
                tbl.add_column("Error")

                # Try to extract file path from the SQL for better error messages
                file_path = ""
                csv_match = re.search(r"read_csv_auto\('([^']+)'\)", sql)
                json_match = re.search(r"read_json_auto\('([^']+)'\)", sql)
                file_match = re.search(r"FROM '([^']+)'", sql)

                if csv_match:
                    file_path = csv_match.group(1)
                elif json_match:
                    file_path = json_match.group(1)
                elif file_match:
                    file_path = file_match.group(1)

                error_message = self._classify_file_error(e, file_path) if file_path else str(e)

                tbl.add_row(error_message)
                meta.update("Error occurred")
                tab.last_status = "error"
                self._update_tab_label(tab_pane_id, "error")
                if not self.is_any_query_running(exclude_task=current_task):
                    self.set_status("error")
        finally:
            if tab.running_task is current_task:
                tbl.loading = False
                tab.running_task = None  # Clear the running task reference
                logger.debug("Query execution completed, task reference cleared")

    def action_save_query(self):
        """Save the current query with a name."""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active:
            return

        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        sql = tab.query_one(TextArea).text.strip()
        if not sql:
            return

        def handle_result(result):
            if result and result.get("saved"):
                # Reload the config and refresh the UI
                self.config = load_config()
                self.load_saved_queries_list()

        self.push_screen(SaveQueryDialog(sql), handle_result)

    def load_query_in_current_tab(self, sql: str):
        """Load a query into the current active tab."""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active:
            return

        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        textarea = tab.query_one(TextArea)
        textarea.text = sql
        textarea.selection = Selection((0, 0), (0, 0))
        textarea.focus()

    def load_history_list(self):
        """Load history into the history tree."""
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
        """Load saved queries into the saved queries tree."""
        tree = self.query_one("#saved-queries-tree", Tree)
        tree.root.expand()
        for child in list(tree.root.children):
            child.remove()
        for i, item in enumerate(self.config["saved_queries"]):
            timestamp = datetime.fromisoformat(item["timestamp"]).strftime("%d/%m %H:%M")
            label = f"{item['name']} [{timestamp}]"
            tree.root.add_leaf(label, data={"type": "saved", "sql": item["sql"]})

    def refresh_tab_table(self, tab: QueryTab):
        logger.debug(f"Refreshing table: {len(tab.full_data)} rows, {len(tab.column_names)} columns")

        tbl, data = tab.query_one(DataTable), [r[:] for r in tab.full_data]

        # Apply filters first
        for i, s in tab.col_states.items():
            if s["filter"]:
                f = s["filter"].lower()
                initial_count = len(data)
                data = [r for r in data if f in str(r[i]).lower()]
                logger.debug(f"Applied filter on column {i}: {initial_count} -> {len(data)} rows")

        # Apply sorting: sort None values safely without TypeError
        sort_applied = False
        for i, s in tab.col_states.items():
            if s["sort"]:
                initial_count = len(data)
                try:
                    data.sort(key=lambda x: (x[i] is None, x[i]), reverse=(s["sort"] == "desc"))
                except TypeError:
                    data.sort(key=lambda x: (x[i] is None, str(x[i])), reverse=(s["sort"] == "desc"))
                sort_applied = True
                logger.debug(f"Applied sort on column {i} ({tab.column_names[i] if i < len(tab.column_names) else 'N/A'}): {s['sort']}, {initial_count} rows sorted")
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
            logger.debug(f"Added column {i}: {lbl}")

        # Add data rows
        logger.debug(f"Adding {len(data)} data rows to table")
        for r in data:
            tbl.add_row(*[str(v) if v is not None else "NULL" for v in r])

        logger.debug(f"Table refresh completed: {len(data)} rows, {len(tab.column_names)} columns")

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected):
        tabs = self.query_one("#tabs", TabbedContent)
        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        idx = int(event.column_key.value)

        logger.debug(f"Header selected: column index {idx}, column name {tab.column_names[idx] if idx < len(tab.column_names) else 'N/A'}")

        # Toggle sort direction: None -> ASC -> DESC -> None
        current_sort = tab.col_states[idx]["sort"]
        logger.debug(f"Current sort state for column {idx}: {current_sort}")

        if current_sort is None:
            new_sort = "asc"
        elif current_sort == "asc":
            new_sort = "desc"
        else:  # current_sort == "desc"
            new_sort = None

        logger.debug(f"New sort state for column {idx}: {new_sort}")

        # Clear all other sorts
        for i in tab.col_states:
            tab.col_states[i]["sort"] = None

        # Apply new sort
        tab.col_states[idx]["sort"] = new_sort
        logger.debug(f"Applied sort: {new_sort} to column {idx}, refreshing table")
        self.refresh_tab_table(tab)

    async def _open_file_path(self, file_path: Path) -> None:
        """Handle opening a file from the file explorer: load into active tab or create new tab."""
        if not is_duckdb_file(file_path):
            return
        try:
            suffix = file_path.suffix.lower()
            if suffix in {".xlsx", ".xls"}:
                table_name = await self._load_and_register_excel(file_path)
                sql = f'SELECT * FROM "{table_name}" LIMIT 100;'
            else:
                sql = sql_for_file(file_path)

            try:
                tabs = self.query_one("#tabs", TabbedContent)
            except Exception:
                if not self.is_any_query_running():
                    self.set_status("done")
                return

            if tabs.active and self._is_tab_empty_and_unsaved(tabs.active):
                active_id = tabs.active
                self._tab_base_names[active_id] = file_path.name
                self.load_query_in_current_tab(sql)
                self.set_status("running", pane_id=active_id)
                self.run_worker(self.action_run_query(pane_id=active_id))
            else:
                await self.add_new_tab(file_path.name, sql, run=True)
        except Exception as e:
            logger.error(f"Error opening file {file_path}: {str(e)}")
            error_msg = self._classify_file_error(e, str(file_path))
            error_sql = f"-- Error opening file: {file_path.name}\n-- {error_msg}"
            try:
                tabs = self.query_one("#tabs", TabbedContent)
                if tabs.active and self._is_tab_empty_and_unsaved(tabs.active):
                    active_id = tabs.active
                    self._tab_base_names[active_id] = file_path.name
                    self.load_query_in_current_tab(error_sql)
                    self.set_status("error", pane_id=active_id)
                else:
                    await self.add_new_tab(file_path.name, error_sql, run=False)
                    self.set_status("error", pane_id=tabs.active)
            except Exception:
                self.set_status("error")

    async def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        await self._open_file_path(Path(event.path))

    on_duck_tree_file_selected = on_directory_tree_file_selected

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Sync status and terminal title when user switches tabs."""
        try:
            pane_id = event.pane.id if event.pane else event.tabbed_content.active
            if pane_id:
                pane = self.query_one(f"#{pane_id}", TabPane)
                qtab = pane.query_one(QueryTab)
                task = getattr(qtab, "running_task", None)
                if task and not task.done():
                    self.set_status("running", pane_id=pane_id)
                elif hasattr(qtab, "last_status"):
                    self.set_status(qtab.last_status, pane_id=pane_id)
                elif self.is_any_query_running():
                    self.set_status("running", pane_id=pane_id)
                else:
                    self.set_status("done", pane_id=pane_id)
        except Exception:
            pass

    def copy_to_clipboard(self, text: str) -> None:
        copy_to_system_clipboard(text)
        super().copy_to_clipboard(text)

    @property
    def clipboard(self) -> str:
        text = get_system_clipboard()
        if text:
            self._clipboard = text
            return text
        return super().clipboard

    def action_toggle_comment(self):
        """Toggle SQL comments in the currently active tab like VS Code."""
        try:
            tabs = self.query_one("#tabs", TabbedContent)
            if not tabs.active:
                return
            tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.action_toggle_comment()
            editor.focus()
        except Exception as e:
            logger.error(f"Error toggling comment: {e}")

    def _enable_smooth_mouse(self):
        """Re-enable standard Textual mouse tracking."""
        if hasattr(self, "_driver") and self._driver:
            self._driver.write("\x1b[?1000h\x1b[?1002h\x1b[?1003h\x1b[?1006h")
            self._driver.flush()

    def _disable_all_mouse(self):
        """Disable all terminal mouse tracking (giving 100% native terminal selection like nano)."""
        if hasattr(self, "_driver") and self._driver:
            self._driver.write("\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l\x1b[?1015l")
            self._driver.flush()

    def action_toggle_mouse(self):
        """Toggle mouse tracking on/off (allowing native terminal selection like nano)."""
        if not hasattr(self, "_mouse_reporting_enabled"):
            self._mouse_reporting_enabled = True

        if self._mouse_reporting_enabled:
            self._disable_all_mouse()
            self._mouse_reporting_enabled = False
            self.notify("Native terminal selectie actief (zoals in nano). Druk nogmaals op Ctrl+M voor TUI muis.", title="Muis-modus", timeout=3)
        else:
            self._enable_smooth_mouse()
            self._mouse_reporting_enabled = True
            self.notify("TUI muis-modus actief (knoppen & tabs klikbaar).", title="Muis-modus", timeout=2)

    def action_about(self):
        self.push_screen(AboutScreen())

    def on_click(self, event: Click) -> None:
        """Handle clicks on tab close buttons (✕ in tab titles)."""
        try:
            widget, _ = self.screen.get_widget_at(event.screen_x, event.screen_y)
        except Exception:
            return
        if isinstance(widget, ContentTab) and widget.label_text.endswith(" ✕"):
            # Check if click was on the ✕ area (last 3 characters of the tab)
            relative_x = event.screen_x - widget.region.x
            if relative_x >= widget.region.width - 3:
                pane_id = ContentTab.sans_prefix(widget.id)
                self._close_tab_by_id(pane_id)
                event.stop()

    def action_undo(self):
        """Undo last action in current query editor."""
        try:
            tabs = self.query_one("#tabs", TabbedContent)
            tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.action_undo()
        except Exception as e:
            logger.error(f"Error executing undo: {e}")

    def action_redo(self):
        """Redo last undone action in current query editor."""
        try:
            tabs = self.query_one("#tabs", TabbedContent)
            tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
            editor = tab.query_one(SqlTextArea)
            editor.action_redo()
        except Exception as e:
            logger.error(f"Error executing redo: {e}")

    def on_key(self, event) -> None:
        """Global key handler for shortcuts when focus is outside the editor."""
        if len(self.screen_stack) > 1:
            return
        key = (event.key or "").lower()
        if key in ("super+t", "cmd+t", "command+t", "ctrl+t"):
            self.action_new_tab()
            event.stop()
            event.prevent_default()
        elif key in ("super+z", "cmd+z", "command+z", "ctrl+z"):
            self.action_undo()
            event.stop()
            event.prevent_default()
        elif key in (
            "super+shift+z", "shift+super+z", "cmd+shift+z", "shift+cmd+z",
            "command+shift+z", "shift+command+z", "ctrl+shift+z", "shift+ctrl+z",
            "super+y", "cmd+y", "command+y", "ctrl+y"
        ):
            self.action_redo()
            event.stop()
            event.prevent_default()

    def action_new_tab(self):
        """Open a new empty tab."""
        return self.run_worker(self.add_new_tab("New Tab", ""))

    def action_close_tab(self):
        tabs = self.query_one("#tabs", TabbedContent)
        if tabs.active:
            self._close_tab_by_id(tabs.active)

    def action_quit(self):
        self.exit()

    def on_tree_node_selected(self, event: Tree.NodeSelected):
        """Handle selection in database tree, saved queries tree, and history tree."""
        node = event.node
        if not node.data:
            return
        if not isinstance(node.data, dict):
            path = getattr(node.data, "path", None)
            if path and is_duckdb_file(Path(path)):
                self.run_worker(self._open_file_path(Path(path)))
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

    def action_plot(self) -> None:
        """Open plot dialog for current tab."""
        tabs = self.query_one("#tabs", TabbedContent)
        if not tabs.active:
            return

        tab = self.query_one(f"#{tabs.active}").query_one(QueryTab)
        if not tab.full_data or not tab.column_names:
            meta = tab.query_one("#metadata-bar") if tab else None
            if meta:
                meta.update("No data to plot")
            self.notify("No data to plot. Run a query first.", severity="warning")
            return

        self.push_screen(PlotDialog(tab))

    def on_button_pressed(self, event: Button.Pressed):
        """Handle button presses."""
        if event.button.id == "btn-run-query":
            self.run_worker(self.action_run_query())
            return
        elif event.button.id == "btn-plot":
            self.action_plot()
            return
        elif event.button.id == "btn-comment":
            self.action_toggle_comment()
            return

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
            task = getattr(tab, "running_task", None)
            if task and not task.done():
                logger.debug("Cancelling query task and interrupting DuckDB")
                # Interrupt DuckDB execution
                try:
                    self.con.interrupt()
                except Exception as e:
                    logger.error(f"Error interrupting DuckDB: {e}")

                task.cancel()
                meta = tab.query_one("#metadata-bar")
                meta.update("Query cancelled")
                tbl = tab.query_one(DataTable)
                tbl.loading = False
                if not self.is_any_query_running(exclude_task=task):
                    self.set_status("cancelled")

        elif event.button.id == "close-tab":
            # Close the current tab
            tabs = self.query_one("#tabs", TabbedContent)
            if tabs.active:
                self._close_tab_by_id(tabs.active)

    def on_input_changed(self, event: Input.Changed):
        """Handle search input changes for database tables."""
        if event.input.id == "db-search-input":
            # Find the database tree and filter tables
            try:
                db_tree = self.query_one("#db-tree", DatabaseTree)
                db_tree.filter_tables(event.value)
            except Exception:
                # If the element doesn't exist yet, ignore
                pass
