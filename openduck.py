#!/usr/bin/env python3
"""
OpenDuck - A flat file SQL client based on DuckDB.

Main executable entry point.
"""

from openduck.main import main
from openduck.gui.app import DuckCLI
from openduck.core.config import (
    CWD,
    CONFIG_FILE,
    MAX_UI_ROWS,
    DUCKDB_EXTENSIONS,
    load_config,
    save_config,
    add_to_history,
    save_query,
    save_connection,
    delete_connection,
)
from openduck.core.clipboard import copy_to_system_clipboard, get_system_clipboard
from openduck.core.sql_utils import is_duckdb_file, sql_for_file, toggle_sql_comment_lines
from openduck.core.error_classifier import classify_file_error
from openduck.gui.screens.about import AboutScreen, DUCK_ART, LOGO_RAW, lolcat
from openduck.gui.screens.matrix import MatrixScreen
from openduck.gui.dialogs.save_query_dialog import SaveQueryDialog
from openduck.gui.dialogs.export_dialog import ExportDialog
from openduck.gui.dialogs.add_connection_dialog import AddConnectionDialog
from openduck.gui.widgets.file_tree import DuckTree
from openduck.gui.widgets.database_tree import DatabaseTree
from openduck.gui.widgets.sql_editor import SqlTextArea
from openduck.gui.widgets.results_table import ResultsTable
from openduck.gui.widgets.query_tab import QueryTab
from openduck.gui.widgets.resize_handle import ResizeHandle

__all__ = [
    "main",
    "DuckCLI",
    "CWD",
    "CONFIG_FILE",
    "MAX_UI_ROWS",
    "DUCKDB_EXTENSIONS",
    "load_config",
    "save_config",
    "add_to_history",
    "save_query",
    "save_connection",
    "delete_connection",
    "copy_to_system_clipboard",
    "get_system_clipboard",
    "is_duckdb_file",
    "sql_for_file",
    "toggle_sql_comment_lines",
    "classify_file_error",
    "AboutScreen",
    "DUCK_ART",
    "LOGO_RAW",
    "lolcat",
    "MatrixScreen",
    "SaveQueryDialog",
    "ExportDialog",
    "AddConnectionDialog",
    "DuckTree",
    "DatabaseTree",
    "SqlTextArea",
    "ResultsTable",
    "QueryTab",
    "ResizeHandle",
]

if __name__ == "__main__":
    main()