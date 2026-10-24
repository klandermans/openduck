"""OpenDuck - A flat file SQL client based on DuckDB."""

__version__ = "0.1.6"

from openduck.core.config import (
    CWD,
    CONFIG_FILE,
    MAX_UI_ROWS,
    DUCKDB_EXTENSIONS,
    add_to_history,
    delete_connection,
    load_config,
    save_config,
    save_connection,
    save_query,
)
from openduck.core.error_classifier import classify_file_error
from openduck.core.sql_utils import is_duckdb_file, sql_for_file, toggle_sql_comment_lines
from openduck.core.clipboard import copy_to_system_clipboard, get_system_clipboard
from openduck.gui.app import DuckCLI
from openduck.main import main

__all__ = [
    "DuckCLI",
    "main",
    "load_config",
    "save_config",
    "add_to_history",
    "save_query",
    "save_connection",
    "delete_connection",
    "classify_file_error",
    "is_duckdb_file",
    "sql_for_file",
    "toggle_sql_comment_lines",
    "copy_to_system_clipboard",
    "get_system_clipboard",
    "CWD",
    "CONFIG_FILE",
    "MAX_UI_ROWS",
    "DUCKDB_EXTENSIONS",
]
