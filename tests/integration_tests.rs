use openduck::app::{App, Focus};
use openduck::config::{add_to_history, save_query};
use openduck::db::DbEngine;
use openduck::editor::Editor;
use openduck::sql_utils::{classify_file_error, sql_for_file};
use openduck::table::TableView;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn test_duckdb_csv_query_and_sorting() {
    let mut engine = DbEngine::new().expect("db engine init");
    let res = engine.execute_query("SELECT * FROM read_csv_auto('samples/ducks.csv') ORDER BY common_name ASC;");
    assert!(res.error.is_none(), "Query error: {:?}", res.error);
    assert_eq!(res.columns, vec!["id", "species", "common_name", "habitat", "wingspan_cm", "migratory"]);
    assert_eq!(res.row_count, 8);

    let mut table = TableView::new();
    table.set_data(res.columns, res.rows);
    assert_eq!(table.row_count(), 8);

    // Sort by wingspan_cm (col 4)
    table.toggle_sort(4);
    let span_0: i32 = table.rows[0][4].parse().unwrap();
    let span_last: i32 = table.rows[table.row_count() - 1][4].parse().unwrap();
    assert!(span_0 <= span_last);

    // Toggle sort to descending
    table.toggle_sort(4);
    let desc_0: i32 = table.rows[0][4].parse().unwrap();
    let desc_last: i32 = table.rows[table.row_count() - 1][4].parse().unwrap();
    assert!(desc_0 >= desc_last);
}

#[test]
fn test_app_full_workflow() {
    let mut app = App::new().expect("app init");
    assert_eq!(app.tabs.len(), 1);

    // Open ducks.csv
    let ducks_path = PathBuf::from("samples/ducks.csv");
    app.open_file(&ducks_path);

    assert_eq!(app.current_tab().title, "ducks.csv");
    assert!(app.current_tab().editor.get_text().contains("read_csv_auto('samples/ducks.csv')"));
    assert_eq!(app.current_tab().table.row_count(), 8);
    assert_eq!(app.current_tab().table.columns, vec!["id", "species", "common_name", "habitat", "wingspan_cm", "migratory"]);

    // Test creating another tab
    app.new_tab(Some("Sales"), Some("SELECT * FROM read_csv_auto('samples/sales.csv') LIMIT 3;"));
    app.run_query();
    assert_eq!(app.tabs.len(), 2);
    assert_eq!(app.current_tab().title, "Sales");
    assert_eq!(app.current_tab().table.row_count(), 3);

    // Test switching tabs with Alt+Left
    let alt_left = KeyEvent::new(KeyCode::Left, KeyModifiers::ALT);
    app.handle_key(alt_left).unwrap();
    assert_eq!(app.active_tab, 0);
    assert_eq!(app.current_tab().title, "ducks.csv");

    // Test comment toggle in editor
    let f3_key = KeyEvent::new(KeyCode::F(3), KeyModifiers::NONE);
    app.focus = Focus::Editor;
    let old_line = app.current_tab().editor.lines[0].clone();
    app.handle_key(f3_key).unwrap();
    assert!(app.current_tab().editor.lines[0].starts_with("--"));
    app.handle_key(f3_key).unwrap();
    assert_eq!(app.current_tab().editor.lines[0], old_line);

    // Test CSV export from table
    let temp_dir = tempfile::tempdir().unwrap();
    let export_file = temp_dir.path().join("exported_ducks.csv");
    let count = app.current_tab().table.export_csv(&export_file).unwrap();
    assert_eq!(count, 8);
    let exported_text = fs::read_to_string(&export_file).unwrap();
    assert!(exported_text.contains("id,species,common_name,habitat,wingspan_cm,migratory"));
    assert!(exported_text.contains("Mallard"));

    // Test copy table as TSV
    let tsv = app.current_tab().table.copy_entire_table().unwrap();
    assert!(tsv.contains("id\tspecies\tcommon_name\thabitat\twingspan_cm\tmigratory"));
    assert!(tsv.contains("Mallard"));

    // Test closing tab
    let ctrl_w = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL);
    app.handle_key(ctrl_w).unwrap();
    assert_eq!(app.tabs.len(), 1);
    assert_eq!(app.current_tab().title, "Sales");
}

#[test]
fn test_config_history_and_save_query() {
    let sql_test = "SELECT 'test_query_persistence' AS col;";
    let cfg = add_to_history(sql_test).unwrap();
    assert!(cfg.history.iter().any(|h| h.sql == sql_test));

    let saved_name = "My Test Query";
    let cfg2 = save_query(saved_name, sql_test).unwrap();
    let item = cfg2.saved_queries.iter().find(|q| q.name == saved_name);
    assert!(item.is_some());
    assert_eq!(item.unwrap().sql, sql_test);
}

#[test]
fn test_sql_utils_mappings() {
    assert_eq!(
        sql_for_file(Path::new("test.parquet")),
        "SELECT * FROM 'test.parquet' LIMIT 100;"
    );
    assert_eq!(
        sql_for_file(Path::new("test.json")),
        "SELECT * FROM read_json_auto('test.json') LIMIT 100;"
    );
    assert_eq!(
        sql_for_file(Path::new("test.jsonl")),
        "SELECT * FROM read_json_auto('test.jsonl') LIMIT 100;"
    );
    assert_eq!(
        sql_for_file(Path::new("test.sqlite")),
        "INSTALL sqlite;\nLOAD sqlite;\nATTACH 'test.sqlite' AS sqlite_db (TYPE SQLITE);\nSHOW TABLES FROM sqlite_db;"
    );
    assert_eq!(
        sql_for_file(Path::new("test.duckdb")),
        "ATTACH 'test.duckdb' AS other;\nSHOW TABLES;"
    );
    assert_eq!(
        sql_for_file(Path::new("test-data.xlsx")),
        "SELECT * FROM test_data LIMIT 100;"
    );
}

#[test]
fn test_error_classifier_diagnostics() {
    let not_found_err = classify_file_error("Could not open file: No such file or directory", "missing.csv");
    assert!(not_found_err.contains("File not found"));
    assert!(not_found_err.contains("missing.csv"));

    let perm_err = classify_file_error("Permission denied reading file", "secret.parquet");
    assert!(perm_err.contains("Permission denied"));

    let json_err = classify_file_error("JSON parser error at line 5", "bad.json");
    assert!(json_err.contains("invalid JSON syntax"));
}

#[test]
fn test_editor_multiline_editing() {
    let mut editor = Editor::new("SELECT 1;");
    editor.move_end();
    editor.insert_newline();
    for c in "SELECT 2;".chars() {
        editor.insert_char(c);
    }
    assert_eq!(editor.lines.len(), 2);
    assert_eq!(editor.lines[0], "SELECT 1;");
    assert_eq!(editor.lines[1], "SELECT 2;");

    editor.undo();
    // After multiple operations undo walks backward
    assert_eq!(editor.lines.len(), 2);
}

#[test]
fn test_tab_pristine_and_opening_files() {
    let mut app = App::new().expect("app init");
    assert_eq!(app.tabs.len(), 1);
    assert!(app.current_tab().is_pristine);

    // Opening first file replaces pristine initial tab
    let ducks_path = PathBuf::from("samples/ducks.csv");
    app.open_file(&ducks_path);
    assert_eq!(app.tabs.len(), 1);
    assert_eq!(app.current_tab().title, "ducks.csv");
    assert!(!app.current_tab().is_pristine);

    // Opening second file MUST open a brand new tab because current tab is no longer pristine
    let sales_path = PathBuf::from("samples/sales.csv");
    app.open_file(&sales_path);
    assert_eq!(app.tabs.len(), 2);
    assert_eq!(app.current_tab().title, "sales.csv");

    // Close active tab
    app.close_active_tab();
    assert_eq!(app.tabs.len(), 1);
    assert_eq!(app.current_tab().title, "ducks.csv");

    // Close last remaining tab: should reset to clean pristine tab rather than leaving app in an invalid state
    app.close_active_tab();
    assert_eq!(app.tabs.len(), 1);
    assert_eq!(app.current_tab().title, "Query 1");
    assert!(app.current_tab().is_pristine);
}

#[test]
fn test_table_2d_scrolling_and_visible_slice() {
    let mut table = TableView::new();
    let cols: Vec<String> = (0..30).map(|i| format!("col_{}", i)).collect();
    let rows: Vec<Vec<String>> = (0..50)
        .map(|r| (0..30).map(|c| format!("val_{}_{}", r, c)).collect())
        .collect();
    table.set_data(cols, rows);

    // Initial slice at top-left
    let (start_row, end_row, start_col, end_col) = table.visible_slice(10, 16, 80);
    assert_eq!(start_row, 0);
    assert_eq!(end_row, 10);
    assert_eq!(start_col, 0);
    assert_eq!(end_col, 5); // 80 / 16 = 5 visible cols

    // Move right past visible columns: column 8
    table.selected_col = 8;
    table.scroll_col = start_col;
    let (_, _, s_col, e_col) = table.visible_slice(10, 16, 80);
    assert!(s_col <= 8 && 8 < e_col, "Selected col 8 must be within visible slice [{}, {})", s_col, e_col);

    // Move down past visible rows: row 25
    table.selected_row = 25;
    table.scroll_row = start_row;
    let (s_row, e_row, _, _) = table.visible_slice(10, 16, 80);
    assert!(s_row <= 25 && 25 < e_row, "Selected row 25 must be within visible slice [{}, {})", s_row, e_row);
}

#[test]
fn test_mouse_toggle_and_scrolling() {
    let mut app = App::new().expect("app init");
    assert!(app.mouse_enabled);

    // Toggle mouse off via shortcut Ctrl+M
    let ctrl_m = KeyEvent::new(KeyCode::Char('m'), KeyModifiers::CONTROL);
    app.handle_key(ctrl_m).unwrap();
    assert!(!app.mouse_enabled);
    assert!(app.status_message.contains("disabled"));

    // Toggle mouse back on
    app.handle_key(ctrl_m).unwrap();
    assert!(app.mouse_enabled);
    assert!(app.status_message.contains("enabled"));
}

