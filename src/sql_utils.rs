use std::path::Path;

use crate::config::is_duckdb_extension;

pub fn is_duckdb_file(path: &Path) -> bool {
    path.is_file()
        && path
            .file_name()
            .and_then(|n| n.to_str())
            .map(is_duckdb_extension)
            .unwrap_or(false)
}

pub fn sql_for_file(path: &Path) -> String {
    let p = path.to_string_lossy().replace('\\', "/");
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_lowercase();

    if filename.ends_with(".duckdb") {
        return format!("ATTACH '{}' AS other;\nSHOW TABLES;", p);
    }
    if filename.ends_with(".sqlite") || filename.ends_with(".sqlite3") || filename.ends_with(".db") {
        return format!(
            "INSTALL sqlite;\nLOAD sqlite;\nATTACH '{}' AS sqlite_db (TYPE SQLITE);\nSHOW TABLES FROM sqlite_db;",
            p
        );
    }
    if filename.ends_with(".xlsx") || filename.ends_with(".xls") {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table");
        let mut table_name = stem.replace([' ', '-'], "_");
        // Replace non-alphanumeric/underscore
        table_name = table_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();
        if table_name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            table_name = format!("_{}", table_name);
        }
        return format!("SELECT * FROM {} LIMIT 100;", table_name);
    }
    if filename.ends_with(".csv") || filename.ends_with(".csv.gz") {
        return format!("SELECT * FROM read_csv_auto('{}') LIMIT 100;", p);
    }
    if filename.ends_with(".json") || filename.ends_with(".jsonl") {
        return format!("SELECT * FROM read_json_auto('{}') LIMIT 100;", p);
    }

    format!("SELECT * FROM '{}' LIMIT 100;", p)
}

pub fn toggle_sql_comment_lines(lines: &[String]) -> Vec<String> {
    let non_empty: Vec<&String> = lines.iter().filter(|l| !l.trim().is_empty()).collect();
    if non_empty.is_empty() {
        return lines
            .iter()
            .map(|l| {
                if !l.starts_with("--") {
                    format!("-- {}", l)
                } else {
                    l.clone()
                }
            })
            .collect();
    }

    let all_commented = non_empty.iter().all(|l| l.trim_start().starts_with("--"));
    let mut result = Vec::with_capacity(lines.len());

    if all_commented {
        for l in lines {
            if l.trim().is_empty() {
                result.push(l.clone());
                continue;
            }
            let trimmed = l.trim_start();
            let indent_len = l.len() - trimmed.len();
            let indent = &l[..indent_len];

            if let Some(stripped) = trimmed.strip_prefix("-- ") {
                result.push(format!("{}{}", indent, stripped));
            } else if let Some(stripped) = trimmed.strip_prefix("--") {
                result.push(format!("{}{}", indent, stripped));
            } else {
                result.push(l.clone());
            }
        }
    } else {
        for l in lines {
            if l.trim().is_empty() {
                result.push(l.clone());
                continue;
            }
            let trimmed = l.trim_start();
            let indent_len = l.len() - trimmed.len();
            let indent = &l[..indent_len];
            result.push(format!("{}-- {}", indent, trimmed));
        }
    }

    result
}

pub fn classify_file_error(err_str: &str, file_path: &str) -> String {
    let lower = err_str.to_lowercase();

    if lower.contains("csv") || lower.contains("read_csv") {
        if lower.contains("no such file") || lower.contains("could not open") {
            format!(
                "❌ File not found: {}\n→ Check if the file exists and the path is correct",
                file_path
            )
        } else if lower.contains("permission") {
            format!(
                "❌ Permission denied: {}\n→ Check file permissions",
                file_path
            )
        } else if lower.contains("encoding") {
            "❌ Encoding error in CSV file\n→ The CSV file may have encoding issues. Try saving it with UTF-8 encoding".to_string()
        } else if lower.contains("delimiter") || lower.contains("separator") {
            "❌ CSV format error\n→ The file may have inconsistent delimiters. Check the CSV format".to_string()
        } else if lower.contains("empty") || lower.contains("no columns") || lower.contains("no rows") {
            "❌ Empty or invalid CSV file\n→ The file appears to be empty or has no valid columns".to_string()
        } else {
            format!(
                "❌ Cannot read CSV file: {}\n→ The file may be corrupted, empty, or in an unsupported format\n\nDetails: {}",
                file_path, err_str
            )
        }
    } else if lower.contains("excel") || lower.contains("xlsx") || lower.contains("xls") {
        if lower.contains("no such file") {
            format!("❌ Excel file not found: {}", file_path)
        } else if lower.contains("permission") {
            format!("❌ Permission denied: {}", file_path)
        } else {
            format!(
                "❌ Cannot read Excel file: {}\n→ The file may be corrupted, password-protected, or in an unsupported format\n\nDetails: {}",
                file_path, err_str
            )
        }
    } else if lower.contains("parquet") || lower.contains("arrow") {
        format!(
            "❌ Cannot read Parquet/Arrow file: {}\n→ The file may be corrupted or in an incompatible format\n\nDetails: {}",
            file_path, err_str
        )
    } else if lower.contains("json") {
        format!(
            "❌ Cannot read JSON file: {}\n→ The file may have invalid JSON syntax\n\nDetails: {}",
            file_path, err_str
        )
    } else if lower.contains("no such file") || lower.contains("does not exist") {
        format!(
            "❌ File not found: {}\n→ The file does not exist at this location",
            file_path
        )
    } else if lower.contains("permission") {
        format!(
            "❌ Permission denied: {}\n→ You don't have permission to access this file",
            file_path
        )
    } else if lower.contains("disk i/o") || lower.contains("io error") {
        "❌ Disk I/O error\n→ There may be a problem with the disk or file system".to_string()
    } else if lower.contains("out of memory") {
        "❌ Out of memory\n→ The file is too large to load into memory".to_string()
    } else if lower.contains("empty") || lower.contains("no columns") {
        "❌ Empty or invalid file\n→ The file appears to be empty or has no valid data".to_string()
    } else {
        format!("❌ Error executing query\n\nDetails: {}", err_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_for_file_csv() {
        let p = Path::new("samples/ducks.csv");
        assert_eq!(
            sql_for_file(p),
            "SELECT * FROM read_csv_auto('samples/ducks.csv') LIMIT 100;"
        );
    }

    #[test]
    fn test_sql_for_file_parquet() {
        let p = Path::new("data/file.parquet");
        assert_eq!(
            sql_for_file(p),
            "SELECT * FROM 'data/file.parquet' LIMIT 100;"
        );
    }

    #[test]
    fn test_sql_for_file_duckdb() {
        let p = Path::new("data/my.duckdb");
        assert_eq!(
            sql_for_file(p),
            "ATTACH 'data/my.duckdb' AS other;\nSHOW TABLES;"
        );
    }

    #[test]
    fn test_toggle_sql_comment_lines() {
        let lines = vec![
            "SELECT *".to_string(),
            "  FROM ducks".to_string(),
        ];
        let commented = toggle_sql_comment_lines(&lines);
        assert_eq!(commented, vec!["-- SELECT *", "  -- FROM ducks"]);

        let uncommented = toggle_sql_comment_lines(&commented);
        assert_eq!(uncommented, vec!["SELECT *", "  FROM ducks"]);
    }
}
