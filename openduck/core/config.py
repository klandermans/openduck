"""Configuration and persistence management for OpenDuck."""

from datetime import datetime
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

CWD = Path(os.getcwd())
CONFIG_FILE = CWD / "openduck.json"
MAX_UI_ROWS = 10000
DUCKDB_EXTENSIONS = {
    ".parquet", ".arrow", ".duckdb", ".csv", ".csv.gz",
    ".json", ".jsonl", ".sqlite", ".sqlite3", ".db", ".xlsx", ".xls"
}


def load_config() -> dict:
    """Load config from openduck.json, create if doesn't exist or is invalid."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
                if isinstance(config, dict):
                    if "connections" not in config:
                        config["connections"] = []
                    if "history" not in config:
                        config["history"] = []
                    if "saved_queries" not in config:
                        config["saved_queries"] = []
                    return config
        except Exception as e:
            logger.error(f"Failed to read config file, falling back to default: {e}")

    default_config = {
        "history": [],
        "saved_queries": [],
        "connections": []
    }
    save_config(default_config)
    return default_config


def save_config(config: dict) -> None:
    """Save config to openduck.json."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def add_to_history(sql: str) -> dict:
    """Add a query to history in the config file, capped at the last 200 queries."""
    config = load_config()
    timestamp = datetime.now().isoformat()
    config["history"].append({
        "sql": sql.strip(),
        "timestamp": timestamp
    })
    # Keep only the last 200 queries to prevent boundless file growth
    config["history"] = config["history"][-200:]
    save_config(config)
    return config


def save_query(name: str, sql: str) -> dict:
    """Save a query with a name to the config file."""
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


def save_connection(conn_info: dict) -> dict:
    """Save a database connection to config."""
    config = load_config()
    for i, c in enumerate(config["connections"]):
        if c["id"] == conn_info["id"]:
            config["connections"][i] = conn_info
            save_config(config)
            return config
    config["connections"].append(conn_info)
    save_config(config)
    return config


def delete_connection(conn_id: str) -> dict:
    """Delete a database connection from config."""
    config = load_config()
    config["connections"] = [c for c in config["connections"] if c["id"] != conn_id]
    save_config(config)
    return config
