"""Core modules for OpenDuck: config, clipboard, SQL utilities, and error classification."""

from openduck.core.clipboard import copy_to_system_clipboard, get_system_clipboard
from openduck.core.config import (
    CONFIG_FILE,
    CWD,
    DUCKDB_EXTENSIONS,
    MAX_UI_ROWS,
    add_to_history,
    delete_connection,
    load_config,
    save_config,
    save_connection,
    save_query,
)
from openduck.core.error_classifier import classify_file_error
from openduck.core.sql_utils import is_duckdb_file, sql_for_file, toggle_sql_comment_lines

__all__ = [
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
]
