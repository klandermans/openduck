"""SQL utilities, formatting, and file-query mappings."""

from pathlib import Path
import re
from typing import List

from openduck.core.config import DUCKDB_EXTENSIONS


def is_duckdb_file(path: Path) -> bool:
    """Check if the given path is a file supported by DuckDB/OpenDuck."""
    return path.is_file() and any(path.name.lower().endswith(ext) for ext in DUCKDB_EXTENSIONS)


def sql_for_file(path: Path) -> str:
    """Generate the initial SQL query statement to preview a given file."""
    p, s = path.as_posix(), path.suffix.lower()
    if s == ".duckdb":
        return f"ATTACH '{p}' AS other;\nSHOW TABLES;"
    if s in {".sqlite", ".sqlite3", ".db"}:
        return f"INSTALL sqlite;\nLOAD sqlite;\nATTACH '{p}' AS sqlite_db (TYPE SQLITE);\nSHOW TABLES FROM sqlite_db;"
    if s in {".xlsx", ".xls"}:
        table_name = re.sub(r'\W|^(?=\d)', '_', path.stem.replace(' ', '_').replace('-', '_'))
        return f"SELECT * FROM {table_name} LIMIT 100;"
    if s in {".csv", ".csv.gz"}:
        return f"SELECT * FROM read_csv_auto('{p}') LIMIT 100;"
    if s in {".json", ".jsonl"}:
        return f"SELECT * FROM read_json_auto('{p}') LIMIT 100;"
    return f"SELECT * FROM '{p}' LIMIT 100;"


def toggle_sql_comment_lines(lines: List[str]) -> List[str]:
    """Toggle SQL line comments (-- ) like in VS Code."""
    non_empty = [l for l in lines if l.strip()]
    if not non_empty:
        return [("-- " + l if not l.startswith("--") else l) for l in lines]

    all_commented = all(l.lstrip().startswith("--") for l in non_empty)
    result = []
    if all_commented:
        for l in lines:
            if not l.strip():
                result.append(l)
                continue
            indent_len = len(l) - len(l.lstrip())
            indent = l[:indent_len]
            content = l[indent_len:]
            if content.startswith("-- "):
                result.append(indent + content[3:])
            elif content.startswith("--"):
                result.append(indent + content[2:])
            else:
                result.append(l)
    else:
        for l in lines:
            if not l.strip():
                result.append(l)
                continue
            indent_len = len(l) - len(l.lstrip())
            indent = l[:indent_len]
            content = l[indent_len:]
            result.append(indent + "-- " + content)
    return result
