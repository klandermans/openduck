use anyhow::Result;
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

pub const DUCKDB_EXTENSIONS: &[&str] = &[
    ".parquet", ".arrow", ".duckdb", ".csv", ".csv.gz",
    ".json", ".jsonl", ".sqlite", ".sqlite3", ".db", ".xlsx", ".xls",
];

pub const MAX_HISTORY: usize = 200;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryItem {
    pub sql: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SavedQuery {
    pub name: String,
    pub sql: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub history: Vec<HistoryItem>,
    #[serde(default)]
    pub saved_queries: Vec<SavedQuery>,
    #[serde(default)]
    pub connections: Vec<serde_json::Value>,
}

pub fn config_path() -> PathBuf {
    PathBuf::from("openduck.json")
}

pub fn load_config() -> Config {
    let path = config_path();
    if path.exists() {
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(cfg) = serde_json::from_str::<Config>(&content) {
                return cfg;
            }
        }
    }
    let default_cfg = Config::default();
    let _ = save_config(&default_cfg);
    default_cfg
}

pub fn save_config(config: &Config) -> Result<()> {
    let content = serde_json::to_string_pretty(config)?;
    fs::write(config_path(), content)?;
    Ok(())
}

pub fn add_to_history(sql: &str) -> Result<Config> {
    let mut config = load_config();
    let timestamp = Local::now().to_rfc3339();
    config.history.push(HistoryItem {
        sql: sql.trim().to_string(),
        timestamp,
    });
    if config.history.len() > MAX_HISTORY {
        let drain_count = config.history.len() - MAX_HISTORY;
        config.history.drain(0..drain_count);
    }
    save_config(&config)?;
    Ok(config)
}

pub fn save_query(name: &str, sql: &str) -> Result<Config> {
    let mut config = load_config();
    let timestamp = Local::now().to_rfc3339();
    let clean_sql = sql.trim().to_string();

    if let Some(pos) = config.saved_queries.iter().position(|q| q.name == name) {
        config.saved_queries[pos] = SavedQuery {
            name: name.to_string(),
            sql: clean_sql,
            timestamp,
        };
    } else {
        config.saved_queries.push(SavedQuery {
            name: name.to_string(),
            sql: clean_sql,
            timestamp,
        });
    }
    save_config(&config)?;
    Ok(config)
}

pub fn is_duckdb_extension(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    DUCKDB_EXTENSIONS.iter().any(|ext| lower.ends_with(ext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_duckdb_extension() {
        assert!(is_duckdb_extension("sales.csv"));
        assert!(is_duckdb_extension("data.parquet"));
        assert!(is_duckdb_extension("table.ARROW"));
        assert!(is_duckdb_extension("log.jsonl"));
        assert!(is_duckdb_extension("my.sqlite3"));
        assert!(!is_duckdb_extension("code.rs"));
        assert!(!is_duckdb_extension("notes.txt"));
    }
}
