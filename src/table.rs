use anyhow::Result;
use arboard::Clipboard;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct TableView {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub selected_row: usize,
    pub selected_col: usize,
    pub scroll_row: usize,
    pub scroll_col: usize,
    pub sort_col: Option<usize>,
    pub sort_desc: bool,
}

impl TableView {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_data(&mut self, columns: Vec<String>, rows: Vec<Vec<String>>) {
        self.columns = columns;
        self.rows = rows;
        self.selected_row = 0;
        self.selected_col = 0;
        self.scroll_row = 0;
        self.scroll_col = 0;
        self.sort_col = None;
        self.sort_desc = false;
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn col_count(&self) -> usize {
        self.columns.len()
    }

    pub fn move_up(&mut self) {
        if self.selected_row > 0 {
            self.selected_row -= 1;
        }
    }

    pub fn move_down(&mut self) {
        if !self.rows.is_empty() && self.selected_row + 1 < self.rows.len() {
            self.selected_row += 1;
        }
    }

    pub fn move_left(&mut self) {
        if self.selected_col > 0 {
            self.selected_col -= 1;
        }
    }

    pub fn move_right(&mut self) {
        if !self.columns.is_empty() && self.selected_col + 1 < self.columns.len() {
            self.selected_col += 1;
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        self.selected_row = self.selected_row.saturating_sub(page_size);
    }

    pub fn page_down(&mut self, page_size: usize) {
        if !self.rows.is_empty() {
            self.selected_row = (self.selected_row + page_size).min(self.rows.len() - 1);
        }
    }

    pub fn move_top(&mut self) {
        self.selected_row = 0;
    }

    pub fn move_bottom(&mut self) {
        if !self.rows.is_empty() {
            self.selected_row = self.rows.len() - 1;
        }
    }

    pub fn visible_slice(
        &self,
        visible_height: usize,
        col_width: u16,
        total_width: u16,
    ) -> (usize, usize, usize, usize) {
        let rows_count = self.rows.len();
        let cols_count = self.columns.len();

        let visible_cols = total_width
            .checked_div(col_width)
            .unwrap_or(1)
            .max(1) as usize;

        let start_row = if self.selected_row < self.scroll_row {
            self.selected_row
        } else if visible_height > 0 && self.selected_row >= self.scroll_row + visible_height {
            self.selected_row + 1 - visible_height
        } else {
            self.scroll_row
        };
        let start_row = start_row.min(rows_count);
        let end_row = (start_row + visible_height).min(rows_count);

        let start_col = if self.selected_col < self.scroll_col {
            self.selected_col
        } else if visible_cols > 0 && self.selected_col >= self.scroll_col + visible_cols {
            self.selected_col + 1 - visible_cols
        } else {
            self.scroll_col
        };
        let start_col = start_col.min(cols_count);
        let end_col = (start_col + visible_cols).min(cols_count);

        (start_row, end_row, start_col, end_col)
    }

    pub fn toggle_sort(&mut self, col: usize) {
        if col >= self.columns.len() {
            return;
        }
        if self.sort_col == Some(col) {
            if self.sort_desc {
                // Reset sorting
                self.sort_col = None;
                self.sort_desc = false;
                return;
            } else {
                self.sort_desc = true;
            }
        } else {
            self.sort_col = Some(col);
            self.sort_desc = false;
        }

        let desc = self.sort_desc;
        self.rows.sort_by(|a, b| {
            let val_a = a.get(col).map(|s| s.as_str()).unwrap_or("");
            let val_b = b.get(col).map(|s| s.as_str()).unwrap_or("");

            // Try comparing numerically first if possible
            if let (Ok(num_a), Ok(num_b)) = (val_a.parse::<f64>(), val_b.parse::<f64>()) {
                let cmp = num_a.partial_cmp(&num_b).unwrap_or(std::cmp::Ordering::Equal);
                if desc { cmp.reverse() } else { cmp }
            } else {
                let cmp = val_a.cmp(val_b);
                if desc { cmp.reverse() } else { cmp }
            }
        });
    }

    pub fn copy_current_cell(&self) -> Result<String> {
        if self.rows.is_empty() || self.columns.is_empty() {
            return Ok(String::new());
        }
        let cell_val = self
            .rows
            .get(self.selected_row)
            .and_then(|r| r.get(self.selected_col))
            .cloned()
            .unwrap_or_default();

        if let Ok(mut clipboard) = Clipboard::new() {
            let _ = clipboard.set_text(&cell_val);
        }
        Ok(cell_val)
    }

    pub fn copy_current_row(&self) -> Result<String> {
        if self.rows.is_empty() {
            return Ok(String::new());
        }
        let row_val = self
            .rows
            .get(self.selected_row)
            .map(|r| r.join("\t"))
            .unwrap_or_default();

        if let Ok(mut clipboard) = Clipboard::new() {
            let _ = clipboard.set_text(&row_val);
        }
        Ok(row_val)
    }

    pub fn copy_entire_table(&self) -> Result<String> {
        let mut lines = Vec::new();
        if !self.columns.is_empty() {
            lines.push(self.columns.join("\t"));
        }
        for row in &self.rows {
            lines.push(row.join("\t"));
        }
        let full_text = lines.join("\n");
        if let Ok(mut clipboard) = Clipboard::new() {
            let _ = clipboard.set_text(&full_text);
        }
        Ok(full_text)
    }

    pub fn export_csv<P: AsRef<Path>>(&self, path: P) -> Result<usize> {
        let file = File::create(path)?;
        let mut wtr = csv::Writer::from_writer(file);

        if !self.columns.is_empty() {
            wtr.write_record(&self.columns)?;
        }
        for row in &self.rows {
            wtr.write_record(row)?;
        }
        wtr.flush()?;
        Ok(self.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_navigation_and_sorting() {
        let mut table = TableView::new();
        let cols = vec!["id".to_string(), "val".to_string()];
        let rows = vec![
            vec!["2".to_string(), "banana".to_string()],
            vec!["1".to_string(), "apple".to_string()],
            vec!["3".to_string(), "cherry".to_string()],
        ];
        table.set_data(cols, rows);

        assert_eq!(table.row_count(), 3);
        table.toggle_sort(0); // Sort by id ascending
        assert_eq!(table.rows[0][0], "1");
        assert_eq!(table.rows[1][0], "2");
        assert_eq!(table.rows[2][0], "3");

        table.toggle_sort(0); // Sort by id descending
        assert_eq!(table.rows[0][0], "3");
        assert_eq!(table.rows[1][0], "2");
        assert_eq!(table.rows[2][0], "1");
    }

    #[test]
    fn test_export_csv() {
        let mut table = TableView::new();
        table.set_data(
            vec!["col1".to_string(), "col2".to_string()],
            vec![vec!["a".to_string(), "b".to_string()]],
        );
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let count = table.export_csv(tmp.path()).unwrap();
        assert_eq!(count, 1);
        let content = std::fs::read_to_string(tmp.path()).unwrap();
        assert!(content.contains("col1,col2"));
        assert!(content.contains("a,b"));
    }
}
