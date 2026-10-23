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

        let stmts = split_sql_statements(trimmed);
        if stmts.is_empty() {
            return QueryResult::default();
        }

        // Run all statements up to the last one
        for stmt_sql in &stmts[..stmts.len() - 1] {
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

        let last_stmt_sql = stmts.last().unwrap();
        match self.run_statement(last_stmt_sql) {
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

    fn run_statement(&mut self, sql: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        match self.conn.prepare(sql) {
            Ok(mut stmt) => {
                match stmt.query([]) {
                    Ok(mut rows) => {
                        let mut rows_out = Vec::new();
                        let mut detected_col_count: Option<usize> = None;

                        while let Some(row) = rows.next()? {
                            let col_count = *detected_col_count.get_or_insert_with(|| row.as_ref().column_count());
                            let mut row_vec = Vec::with_capacity(col_count);
                            for idx in 0..col_count {
                                let val: Value = row.get(idx)?;
                                let s = format_value(&val);
                                row_vec.push(s);
                            }
                            rows_out.push(row_vec);
                            if rows_out.len() >= 10000 {
                                break;
                            }
                        }

                        // Drop rows to release mutable borrow on stmt
                        drop(rows);

                        let col_count = detected_col_count.unwrap_or_else(|| stmt.column_count());
                        if col_count == 0 {
                            return Ok((
                                vec!["Result".to_string()],
                                vec![vec!["Query executed successfully.".to_string()]],
                            ));
                        }

                        let column_names: Vec<String> = stmt
                            .column_names()
                            .into_iter()
                            .map(|s| s.to_string())
                            .collect();

                        Ok((column_names, rows_out))
                    }
                    Err(_) => {
                        // For statements where query([]) fails, try execute
                        let count = stmt.execute([])?;
                        Ok((
                            vec!["Result".to_string()],
                            vec![vec![format!("Query executed successfully ({} rows affected).", count)]],
                        ))
                    }
                }
            }
            Err(prep_err) => {
                match self.conn.execute_batch(sql) {
                    Ok(_) => Ok((
                        vec!["Result".to_string()],
                        vec![vec!["Query executed successfully.".to_string()]],
                    )),
                    Err(_) => Err(prep_err.into()),
                }
            }
        }
    }
}

pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut dollar_tag: Option<String> = None;

    while i < len {
        let c = chars[i];
        let next = if i + 1 < len { Some(chars[i + 1]) } else { None };

        if in_line_comment {
            current.push(c);
            if c == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if in_block_comment {
            current.push(c);
            if c == '*' && next == Some('/') {
                current.push('/');
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_single_quote {
            current.push(c);
            if c == '\'' {
                if next == Some('\'') {
                    current.push('\'');
                    i += 2;
                    continue;
                } else {
                    in_single_quote = false;
                }
            }
            i += 1;
            continue;
        }

        if in_double_quote {
            current.push(c);
            if c == '"' {
                if next == Some('"') {
                    current.push('"');
                    i += 2;
                    continue;
                } else {
                    in_double_quote = false;
                }
            }
            i += 1;
            continue;
        }

        if let Some(ref tag) = dollar_tag.clone() {
            current.push(c);
            let tag_len = tag.len();
            if c == '$' && i >= tag_len {
                let slice: String = chars[i + 1 - tag_len..=i].iter().collect();
                if &slice == tag {
                    dollar_tag = None;
                }
            }
            i += 1;
            continue;
        }

        if c == '-' && next == Some('-') {
            in_line_comment = true;
            current.push(c);
            current.push('-');
            i += 2;
            continue;
        }

        if c == '/' && next == Some('*') {
            in_block_comment = true;
            current.push(c);
            current.push('*');
            i += 2;
            continue;
        }

        if c == '\'' {
            in_single_quote = true;
            current.push(c);
            i += 1;
            continue;
        }

        if c == '"' {
            in_double_quote = true;
            current.push(c);
            i += 1;
            continue;
        }

        if c == '$' {
            let mut j = i + 1;
            while j < len && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j < len && chars[j] == '$' {
                let tag: String = chars[i..=j].iter().collect();
                dollar_tag = Some(tag);
                current.extend(&chars[i..=j]);
                i = j + 1;
                continue;
            }
        }

        if c == ';' {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(c);
        i += 1;
    }

    let final_stmt = current.trim().to_string();
    if !final_stmt.is_empty() {
        statements.push(final_stmt);
    }

    statements
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
        let res = engine.execute_query("SELECT 42 AS answer, 'OpenDuck' AS name, DATE '2024-01-15' AS d;");
        assert!(res.error.is_none());
        assert_eq!(res.columns, vec!["answer", "name", "d"]);
        assert_eq!(res.row_count, 1);
        println!("d is: {}", res.rows[0][2]);
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

    #[test]
    fn test_execute_query_semicolon_in_string() {
        let mut engine = DbEngine::new().expect("engine init");
        let res = engine.execute_query("SELECT 'hello;world' AS greeting;");
        assert!(res.error.is_none(), "Error was: {:?}", res.error);
        assert_eq!(res.columns, vec!["greeting"]);
        assert_eq!(res.rows[0], vec!["hello;world"]);
    }

    #[test]
    fn test_execute_query_with_comment() {
        let mut engine = DbEngine::new().expect("engine init");
        let res = engine.execute_query("-- this is a comment\nSELECT 100 AS num;");
        assert!(res.error.is_none(), "Error was: {:?}", res.error);
        assert_eq!(res.columns, vec!["num"]);
        assert_eq!(res.rows[0], vec!["100"]);
    }

    #[test]
    fn test_insert_returning_no_double_execution() {
        let mut engine = DbEngine::new().expect("engine init");
        let _ = engine.execute_query("CREATE TABLE counter(x INT);");
        let res = engine.execute_query("INSERT INTO counter VALUES (1) RETURNING *;");
        assert!(res.error.is_none(), "Error was: {:?}", res.error);
        let check = engine.execute_query("SELECT count(*) FROM counter;");
        assert_eq!(check.rows[0][0], "1", "Should have exactly 1 row inserted, not duplicated!");
    }

    #[test]
    fn test_multi_statement_with_semicolon_in_string() {
        let mut engine = DbEngine::new().expect("engine init");
        let sql = "CREATE TABLE animals(id INT, name VARCHAR); INSERT INTO animals VALUES (1, 'Duck;Donald'); SELECT * FROM animals;";
        let res = engine.execute_query(sql);
        assert!(res.error.is_none(), "Error: {:?}", res.error);
        assert_eq!(res.columns, vec!["id", "name"]);
        assert_eq!(res.row_count, 1);
        assert_eq!(res.rows[0], vec!["1", "Duck;Donald"]);
    }
}
