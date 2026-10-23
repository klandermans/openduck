use std::time::Instant;
use duckdb::{Connection, types::Value};
use anyhow::Result;

use crate::sql_utils::classify_file_error;

#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub duration_ms: u128,
    pub row_count: usize,
    pub error: Option<String>,
}

pub struct DbEngine {
    pub conn: Connection,
}

impl DbEngine {
    pub fn new() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    pub fn execute_query(&mut self, sql: &str) -> QueryResult {
        let start = Instant::now();
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return QueryResult {
                columns: vec![],
                rows: vec![],
                duration_ms: 0,
                row_count: 0,
                error: None,
            };
        }

        // Split statements by semicolon if multiple exist
        let raw_stmts: Vec<&str> = trimmed
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if raw_stmts.is_empty() {
            return QueryResult::default();
        }

        // Run all statements up to the last one
        for &stmt_sql in &raw_stmts[..raw_stmts.len() - 1] {
            if let Err(e) = self.conn.execute_batch(stmt_sql) {
                let err_msg = classify_file_error(&e.to_string(), "");
                return QueryResult {
                    columns: vec![],
                    rows: vec![],
                    duration_ms: start.elapsed().as_millis(),
                    row_count: 0,
                    error: Some(err_msg),
                };
            }
        }

        let last_stmt_sql = raw_stmts.last().unwrap();
        // Check if last statement is a query returning rows or an execute
        let lower = last_stmt_sql.to_lowercase();
        let is_query = lower.starts_with("select")
            || lower.starts_with("show")
            || lower.starts_with("describe")
            || lower.starts_with("pragma")
            || lower.starts_with("explain")
            || lower.starts_with("with")
            || lower.starts_with("from");

        if !is_query {
            // DDL / DML execute
            match self.conn.execute_batch(last_stmt_sql) {
                Ok(_) => {
                    return QueryResult {
                        columns: vec!["Result".to_string()],
                        rows: vec![vec!["Query executed successfully.".to_string()]],
                        duration_ms: start.elapsed().as_millis(),
                        row_count: 1,
                        error: None,
                    };
                }
                Err(e) => {
                    let err_msg = classify_file_error(&e.to_string(), "");
                    return QueryResult {
                        columns: vec![],
                        rows: vec![],
                        duration_ms: start.elapsed().as_millis(),
                        row_count: 0,
                        error: Some(err_msg),
                    };
                }
            }
        }

        match self.run_select(last_stmt_sql) {
            Ok((cols, rows)) => {
                let count = rows.len();
                QueryResult {
                    columns: cols,
                    rows,
                    duration_ms: start.elapsed().as_millis(),
                    row_count: count,
                    error: None,
                }
            }
            Err(e) => {
                let err_msg = classify_file_error(&e.to_string(), "");
                QueryResult {
                    columns: vec![],
                    rows: vec![],
                    duration_ms: start.elapsed().as_millis(),
                    row_count: 0,
                    error: Some(err_msg),
                }
            }
        }
    }

    fn run_select(&mut self, sql: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut stmt = self.conn.prepare(sql)?;
        let _ = stmt.execute([])?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let mut rows_out = Vec::new();
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let mut row_vec = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                let val: Value = row.get(idx)?;
                let s = format_value(&val);
                row_vec.push(s);
            }
            rows_out.push(row_vec);
            if rows_out.len() >= 10000 {
                // UI limit
                break;
            }
        }

        Ok((column_names, rows_out))
    }
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::HugeInt(i) => i.to_string(),
        Value::UTinyInt(i) => i.to_string(),
        Value::USmallInt(i) => i.to_string(),
        Value::UInt(i) => i.to_string(),
        Value::UBigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => format!("<BLOB {} bytes>", b.len()),
        Value::Timestamp(unit, val) => format!("Timestamp({:?}, {})", unit, val),
        other => format!("{:?}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_query_select() {
        let mut engine = DbEngine::new().expect("engine init");
        let res = engine.execute_query("SELECT 42 AS answer, 'OpenDuck' AS name;");
        assert!(res.error.is_none());
        assert_eq!(res.columns, vec!["answer", "name"]);
        assert_eq!(res.row_count, 1);
        assert_eq!(res.rows[0], vec!["42", "OpenDuck"]);
    }

    #[test]
    fn test_execute_query_csv() {
        let mut engine = DbEngine::new().expect("engine init");
        let res = engine.execute_query("SELECT * FROM read_csv_auto('samples/ducks.csv') LIMIT 5;");
        assert!(res.error.is_none());
        assert!(!res.columns.is_empty());
        assert!(!res.rows.is_empty());
    }

    #[test]
    fn test_execute_query_error() {
        let mut engine = DbEngine::new().expect("engine init");
        let res = engine.execute_query("SELECT * FROM nonexistent_table;");
        assert!(res.error.is_some());
    }
}
