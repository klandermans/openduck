use crate::sql_utils::toggle_sql_comment_lines;

#[derive(Debug, Clone)]
pub struct Editor {
    pub lines: Vec<String>,
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub scroll_offset: usize,
    undo_stack: Vec<(Vec<String>, usize, usize)>,
    redo_stack: Vec<(Vec<String>, usize, usize)>,
}

impl Default for Editor {
    fn default() -> Self {
        Self::new("")
    }
}

impl Editor {
    pub fn new(text: &str) -> Self {
        let lines: Vec<String> = if text.is_empty() {
            vec![String::new()]
        } else {
            text.lines().map(|s| s.to_string()).collect()
        };
        Self {
            lines,
            cursor_row: 0,
            cursor_col: 0,
            scroll_offset: 0,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        }
    }

    pub fn get_text(&self) -> String {
        self.lines.join("\n")
    }

    pub fn set_text(&mut self, text: &str) {
        self.push_undo();
        self.lines = if text.is_empty() {
            vec![String::new()]
        } else {
            text.lines().map(|s| s.to_string()).collect()
        };
        self.cursor_row = 0;
        self.cursor_col = 0;
        self.scroll_offset = 0;
    }

    fn push_undo(&mut self) {
        self.undo_stack
            .push((self.lines.clone(), self.cursor_row, self.cursor_col));
        self.redo_stack.clear();
        if self.undo_stack.len() > 100 {
            self.undo_stack.remove(0);
        }
    }

    pub fn undo(&mut self) {
        if let Some((prev_lines, prev_row, prev_col)) = self.undo_stack.pop() {
            self.redo_stack
                .push((self.lines.clone(), self.cursor_row, self.cursor_col));
            self.lines = prev_lines;
            self.cursor_row = prev_row.min(self.lines.len().saturating_sub(1));
            self.cursor_col = prev_col.min(self.lines[self.cursor_row].chars().count());
        }
    }

    pub fn redo(&mut self) {
        if let Some((next_lines, next_row, next_col)) = self.redo_stack.pop() {
            self.undo_stack
                .push((self.lines.clone(), self.cursor_row, self.cursor_col));
            self.lines = next_lines;
            self.cursor_row = next_row.min(self.lines.len().saturating_sub(1));
            self.cursor_col = next_col.min(self.lines[self.cursor_row].chars().count());
        }
    }

    pub fn insert_char(&mut self, c: char) {
        self.push_undo();
        if self.cursor_row >= self.lines.len() {
            self.lines.push(String::new());
            self.cursor_row = self.lines.len() - 1;
        }

        let line = &mut self.lines[self.cursor_row];
        let mut chars: Vec<char> = line.chars().collect();
        if self.cursor_col > chars.len() {
            self.cursor_col = chars.len();
        }
        chars.insert(self.cursor_col, c);
        *line = chars.into_iter().collect();
        self.cursor_col += 1;
    }

    pub fn insert_newline(&mut self) {
        self.push_undo();
        let line = &self.lines[self.cursor_row];
        let chars: Vec<char> = line.chars().collect();
        let col = self.cursor_col.min(chars.len());

        let before: String = chars[..col].iter().collect();
        let after: String = chars[col..].iter().collect();

        // Calculate auto-indent from current line
        let indent: String = before
            .chars()
            .take_while(|c| c.is_whitespace())
            .collect();

        self.lines[self.cursor_row] = before;
        let new_line = format!("{}{}", indent, after);
        self.cursor_row += 1;
        self.cursor_col = indent.chars().count();
        self.lines.insert(self.cursor_row, new_line);
    }

    pub fn backspace(&mut self) {
        if self.cursor_col > 0 {
            self.push_undo();
            let line = &mut self.lines[self.cursor_row];
            let mut chars: Vec<char> = line.chars().collect();
            chars.remove(self.cursor_col - 1);
            *line = chars.into_iter().collect();
            self.cursor_col -= 1;
        } else if self.cursor_row > 0 {
            self.push_undo();
            let current_line = self.lines.remove(self.cursor_row);
            self.cursor_row -= 1;
            let prev_line = &mut self.lines[self.cursor_row];
            self.cursor_col = prev_line.chars().count();
            prev_line.push_str(&current_line);
        }
    }

    pub fn delete(&mut self) {
        let line_len = self.lines[self.cursor_row].chars().count();
        if self.cursor_col < line_len {
            self.push_undo();
            let line = &mut self.lines[self.cursor_row];
            let mut chars: Vec<char> = line.chars().collect();
            chars.remove(self.cursor_col);
            *line = chars.into_iter().collect();
        } else if self.cursor_row + 1 < self.lines.len() {
            self.push_undo();
            let next_line = self.lines.remove(self.cursor_row + 1);
            self.lines[self.cursor_row].push_str(&next_line);
        }
    }

    pub fn move_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor_row > 0 {
            self.cursor_row -= 1;
            self.cursor_col = self.lines[self.cursor_row].chars().count();
        }
    }

    pub fn move_right(&mut self) {
        let line_len = self.lines[self.cursor_row].chars().count();
        if self.cursor_col < line_len {
            self.cursor_col += 1;
        } else if self.cursor_row + 1 < self.lines.len() {
            self.cursor_row += 1;
            self.cursor_col = 0;
        }
    }

    pub fn move_up(&mut self) {
        if self.cursor_row > 0 {
            self.cursor_row -= 1;
            let line_len = self.lines[self.cursor_row].chars().count();
            if self.cursor_col > line_len {
                self.cursor_col = line_len;
            }
        }
    }

    pub fn move_down(&mut self) {
        if self.cursor_row + 1 < self.lines.len() {
            self.cursor_row += 1;
            let line_len = self.lines[self.cursor_row].chars().count();
            if self.cursor_col > line_len {
                self.cursor_col = line_len;
            }
        }
    }

    pub fn move_home(&mut self) {
        self.cursor_col = 0;
    }

    pub fn move_end(&mut self) {
        self.cursor_col = self.lines[self.cursor_row].chars().count();
    }

    pub fn toggle_comment(&mut self) {
        self.push_undo();
        let target_line = vec![self.lines[self.cursor_row].clone()];
        let toggled = toggle_sql_comment_lines(&target_line);
        if let Some(first) = toggled.into_iter().next() {
            self.lines[self.cursor_row] = first;
            self.cursor_col = self.lines[self.cursor_row].chars().count();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_editor_insert_and_backspace() {
        let mut editor = Editor::new("SELECT *");
        editor.move_end();
        editor.insert_char(' ');
        editor.insert_char('1');
        assert_eq!(editor.get_text(), "SELECT * 1");
        editor.backspace();
        assert_eq!(editor.get_text(), "SELECT * ");
    }

    #[test]
    fn test_editor_undo_redo() {
        let mut editor = Editor::new("A");
        editor.move_end();
        editor.insert_char('B');
        assert_eq!(editor.get_text(), "AB");
        editor.undo();
        assert_eq!(editor.get_text(), "A");
        editor.redo();
        assert_eq!(editor.get_text(), "AB");
    }

    #[test]
    fn test_editor_comment_toggle() {
        let mut editor = Editor::new("SELECT 1;");
        editor.toggle_comment();
        assert_eq!(editor.get_text(), "-- SELECT 1;");
        editor.toggle_comment();
        assert_eq!(editor.get_text(), "SELECT 1;");
    }
}
