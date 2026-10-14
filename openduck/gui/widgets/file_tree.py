"""Filtered directory tree for DuckDB-supported files."""

from pathlib import Path
from typing import Iterable
from textual.widgets import DirectoryTree

from openduck.core.sql_utils import is_duckdb_file


class DuckTree(DirectoryTree):
    """Directory tree that only shows directories and DuckDB-supported data files."""

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        return [p for p in paths if p.is_dir() or is_duckdb_file(p)]
