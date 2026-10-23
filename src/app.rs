use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::{Path, PathBuf};

use crate::config::{add_to_history, load_config, save_query, Config};
use crate::db::{DbEngine, QueryResult};
use crate::editor::Editor;
use crate::file_tree::FileTree;
use crate::sql_utils::sql_for_file;
use crate::table::TableView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Sidebar,
    Editor,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidebarSection {
    Files,
    SavedQueries,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Modal {
    None,
    SaveQuery { name_input: String },
    ExportCsv { path_input: String },
    About,
    Help,
}

#[derive(Debug, Clone)]
pub struct TabState {
    pub title: String,
    pub editor: Editor,
    pub table: TableView,
    pub duration_ms: u128,
    pub status_text: String,
    pub error: Option<String>,
    pub is_pristine: bool,
}

pub struct App {
    pub tabs: Vec<TabState>,
    pub active_tab: usize,
    pub focus: Focus,
    pub sidebar_section: SidebarSection,
    pub file_tree: FileTree,
    pub saved_queries_index: usize,
    pub saved_queries_scroll: usize,
    pub history_index: usize,
    pub history_scroll: usize,
    pub config: Config,
    pub db: DbEngine,
    pub modal: Modal,
    pub status_message: String,
    pub should_quit: bool,
    pub mouse_enabled: bool,
}

impl App {
    pub fn new() -> Result<Self> {
        let config = load_config();
        let db = DbEngine::new()?;
        let file_tree = FileTree::new(PathBuf::from("."));

        let default_sql = "SELECT 42 AS result, 'OpenDuck' AS client;";
        let mut initial_editor = Editor::new(default_sql);
        initial_editor.move_end();

        let initial_tab = TabState {
            title: "Query 1".to_string(),
            editor: initial_editor,
            table: TableView::new(),
            duration_ms: 0,
            status_text: "Ready".to_string(),
            error: None,
            is_pristine: true,
        };

        let mut app = Self {
            tabs: vec![initial_tab],
            active_tab: 0,
            focus: Focus::Editor,
            sidebar_section: SidebarSection::Files,
            file_tree,
            saved_queries_index: 0,
            saved_queries_scroll: 0,
            history_index: 0,
            history_scroll: 0,
            config,
            db,
            modal: Modal::None,
            status_message: "Ready. Press Ctrl+Enter to run query, F2 for help.".to_string(),
            should_quit: false,
            mouse_enabled: true,
        };

        // Run initial query on the first tab to populate welcome data
        app.run_query();

        Ok(app)
    }

    pub fn current_tab(&self) -> &TabState {
        &self.tabs[self.active_tab]
    }

    pub fn current_tab_mut(&mut self) -> &mut TabState {
        &mut self.tabs[self.active_tab]
    }

    pub fn new_tab(&mut self, title: Option<&str>, initial_sql: Option<&str>) {
        let idx = self.tabs.len() + 1;
        let tab_title = title
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("Query {}", idx));
        let sql = initial_sql.unwrap_or("");
        let mut editor = Editor::new(sql);
        editor.move_end();

        self.tabs.push(TabState {
            title: tab_title,
            editor,
            table: TableView::new(),
            duration_ms: 0,
            status_text: "Ready".to_string(),
            error: None,
            is_pristine: true,
        });
        self.active_tab = self.tabs.len() - 1;
        self.focus = Focus::Editor;
    }

    pub fn close_active_tab(&mut self) {
        if self.tabs.len() > 1 {
            self.tabs.remove(self.active_tab);
            if self.active_tab >= self.tabs.len() {
                self.active_tab = self.tabs.len() - 1;
            }
        } else {
            let tab = self.current_tab_mut();
            tab.title = "Query 1".to_string();
            tab.editor = Editor::new("");
            tab.table = TableView::new();
            tab.duration_ms = 0;
            tab.status_text = "Ready".to_string();
            tab.error = None;
            tab.is_pristine = true;
        }
    }

    pub fn next_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.active_tab = (self.active_tab + 1) % self.tabs.len();
        }
    }

    pub fn prev_tab(&mut self) {
        if !self.tabs.is_empty() {
            if self.active_tab == 0 {
                self.active_tab = self.tabs.len() - 1;
            } else {
                self.active_tab -= 1;
            }
        }
    }

    pub fn run_query(&mut self) {
        let sql = self.current_tab().editor.get_text();
        if sql.trim().is_empty() {
            self.status_message = "Empty query".to_string();
            return;
        }

        self.status_message = "Running query...".to_string();
        let res: QueryResult = self.db.execute_query(&sql);

        // Record into history if successful or attempted
        let _ = add_to_history(&sql);
        self.config = load_config();

        let tab = self.current_tab_mut();
        tab.duration_ms = res.duration_ms;
        if let Some(err) = res.error {
            tab.error = Some(err.clone());
            tab.status_text = "Query failed".to_string();
            self.status_message = format!("❌ Error: {}", err.lines().next().unwrap_or(&err));
        } else {
            tab.error = None;
            tab.status_text = format!("Done in {}ms ({} rows)", res.duration_ms, res.row_count);
            tab.table.set_data(res.columns, res.rows);
            self.status_message = format!(
                "🟢 Success: {} rows returned in {}ms",
                tab.table.row_count(),
                res.duration_ms
            );
        }
    }

    pub fn open_file(&mut self, path: &Path) {
        let sql = sql_for_file(path);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Query");

        let is_empty_or_pristine = self.current_tab().is_pristine
            || self.current_tab().editor.get_text().trim().is_empty();

        if is_empty_or_pristine {
            let tab = self.current_tab_mut();
            tab.title = name.to_string();
            tab.editor.set_text(&sql);
            tab.is_pristine = false;
        } else {
            self.new_tab(Some(name), Some(&sql));
            self.current_tab_mut().is_pristine = false;
        }
        self.run_query();
    }

    pub fn toggle_mouse(&mut self) {
        self.mouse_enabled = !self.mouse_enabled;
        if self.mouse_enabled {
            let _ = crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture);
            self.status_message = "Mouse mode enabled".to_string();
        } else {
            let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
            self.status_message = "Mouse mode disabled (text copy enabled)".to_string();
        }
    }

    pub fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) -> Result<()> {
        if !self.mouse_enabled {
            return Ok(());
        }
        use crossterm::event::MouseEventKind;
        match mouse.kind {
            MouseEventKind::ScrollUp => match self.focus {
                Focus::Sidebar => match self.sidebar_section {
                    SidebarSection::Files => self.file_tree.move_up(),
                    SidebarSection::SavedQueries => {
                        if self.saved_queries_index > 0 {
                            self.saved_queries_index -= 1;
                        }
                    }
                    SidebarSection::History => {
                        if self.history_index > 0 {
                            self.history_index -= 1;
                        }
                    }
                },
                Focus::Editor => self.current_tab_mut().editor.move_up(),
                Focus::Table => self.current_tab_mut().table.move_up(),
            },
            MouseEventKind::ScrollDown => match self.focus {
                Focus::Sidebar => match self.sidebar_section {
                    SidebarSection::Files => self.file_tree.move_down(),
                    SidebarSection::SavedQueries => {
                        if !self.config.saved_queries.is_empty()
                            && self.saved_queries_index + 1 < self.config.saved_queries.len()
                        {
                            self.saved_queries_index += 1;
                        }
                    }
                    SidebarSection::History => {
                        if !self.config.history.is_empty()
                            && self.history_index + 1 < self.config.history.len()
                        {
                            self.history_index += 1;
                        }
                    }
                },
                Focus::Editor => self.current_tab_mut().editor.move_down(),
                Focus::Table => self.current_tab_mut().table.move_down(),
            },
            MouseEventKind::Down(crossterm::event::MouseButton::Left) => {
                if mouse.column < 32 {
                    self.focus = Focus::Sidebar;
                    if mouse.row < 14 {
                        self.sidebar_section = SidebarSection::Files;
                    } else if mouse.row < 22 {
                        self.sidebar_section = SidebarSection::SavedQueries;
                    } else {
                        self.sidebar_section = SidebarSection::History;
                    }
                } else if mouse.row <= 3 {
                    self.next_tab();
                } else if mouse.row < 14 {
                    self.focus = Focus::Editor;
                } else {
                    self.focus = Focus::Table;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        // First handle open modals
        if self.modal != Modal::None {
            self.handle_modal_key(key);
            return Ok(());
        }

        // Global shortcuts
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('q') => {
                    self.should_quit = true;
                    return Ok(());
                }
                KeyCode::Char('s') => {
                    self.modal = Modal::SaveQuery {
                        name_input: String::new(),
                    };
                    return Ok(());
                }
                KeyCode::Char('t') => {
                    self.new_tab(None, None);
                    return Ok(());
                }
                KeyCode::Char('w') => {
                    self.close_active_tab();
                    return Ok(());
                }
                KeyCode::Char('m') => {
                    self.toggle_mouse();
                    return Ok(());
                }
                KeyCode::Char('e') => {
                    self.modal = Modal::ExportCsv {
                        path_input: "export.csv".to_string(),
                    };
                    return Ok(());
                }
                KeyCode::Char('z') => {
                    self.current_tab_mut().editor.undo();
                    return Ok(());
                }
                KeyCode::Char('y') => {
                    self.current_tab_mut().editor.redo();
                    return Ok(());
                }
                KeyCode::Char('/') => {
                    self.current_tab_mut().editor.toggle_comment();
                    return Ok(());
                }
                KeyCode::Enter => {
                    self.run_query();
                    return Ok(());
                }
                KeyCode::PageDown => {
                    self.next_tab();
                    return Ok(());
                }
                KeyCode::PageUp => {
                    self.prev_tab();
                    return Ok(());
                }
                _ => {}
            }
        }

        // Alt shortcuts for tab switching
        if key.modifiers.contains(KeyModifiers::ALT) {
            match key.code {
                KeyCode::Right => {
                    self.next_tab();
                    return Ok(());
                }
                KeyCode::Left => {
                    self.prev_tab();
                    return Ok(());
                }
                _ => {}
            }
        }

        // Function keys
        match key.code {
            KeyCode::F(1) => {
                self.modal = Modal::About;
                return Ok(());
            }
            KeyCode::F(2) => {
                self.modal = Modal::Help;
                return Ok(());
            }
            KeyCode::F(3) => {
                self.current_tab_mut().editor.toggle_comment();
                return Ok(());
            }
            KeyCode::F(5) => {
                self.run_query();
                return Ok(());
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Sidebar => Focus::Editor,
                    Focus::Editor => Focus::Table,
                    Focus::Table => Focus::Sidebar,
                };
                return Ok(());
            }
            KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Sidebar => Focus::Table,
                    Focus::Editor => Focus::Sidebar,
                    Focus::Table => Focus::Editor,
                };
                return Ok(());
            }
            _ => {}
        }

        // Focus-specific key handling
        match self.focus {
            Focus::Sidebar => self.handle_sidebar_key(key),
            Focus::Editor => self.handle_editor_key(key),
            Focus::Table => self.handle_table_key(key),
        }

        Ok(())
    }

    fn handle_sidebar_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('1') => self.sidebar_section = SidebarSection::Files,
            KeyCode::Char('2') => self.sidebar_section = SidebarSection::SavedQueries,
            KeyCode::Char('3') => self.sidebar_section = SidebarSection::History,
            KeyCode::Up => match self.sidebar_section {
                SidebarSection::Files => self.file_tree.move_up(),
                SidebarSection::SavedQueries => {
                    if self.saved_queries_index > 0 {
                        self.saved_queries_index -= 1;
                    }
                }
                SidebarSection::History => {
                    if self.history_index > 0 {
                        self.history_index -= 1;
                    }
                }
            },
            KeyCode::Down => match self.sidebar_section {
                SidebarSection::Files => self.file_tree.move_down(),
                SidebarSection::SavedQueries => {
                    if !self.config.saved_queries.is_empty()
                        && self.saved_queries_index + 1 < self.config.saved_queries.len()
                    {
                        self.saved_queries_index += 1;
                    }
                }
                SidebarSection::History => {
                    if !self.config.history.is_empty()
                        && self.history_index + 1 < self.config.history.len()
                    {
                        self.history_index += 1;
                    }
                }
            },
            KeyCode::Enter => match self.sidebar_section {
                SidebarSection::Files => {
                    if let Some(file_path) = self.file_tree.enter_selected() {
                        self.open_file(&file_path);
                    }
                }
                SidebarSection::SavedQueries => {
                    if let Some(query) = self
                        .config
                        .saved_queries
                        .get(self.saved_queries_index)
                        .cloned()
                    {
                        self.new_tab(Some(&query.name), Some(&query.sql));
                        self.run_query();
                    }
                }
                SidebarSection::History => {
                    let rev_index = self
                        .config
                        .history
                        .len()
                        .saturating_sub(1 + self.history_index);
                    if let Some(item) = self.config.history.get(rev_index).cloned() {
                        self.new_tab(Some("History"), Some(&item.sql));
                        self.run_query();
                    }
                }
            },
            _ => {}
        }
    }

    fn handle_editor_key(&mut self, key: KeyEvent) {
        let editor = &mut self.current_tab_mut().editor;
        match key.code {
            KeyCode::Left => editor.move_left(),
            KeyCode::Right => editor.move_right(),
            KeyCode::Up => editor.move_up(),
            KeyCode::Down => editor.move_down(),
            KeyCode::Home => editor.move_home(),
            KeyCode::End => editor.move_end(),
            KeyCode::PageUp => {
                for _ in 0..10 {
                    editor.move_up();
                }
            }
            KeyCode::PageDown => {
                for _ in 0..10 {
                    editor.move_down();
                }
            }
            KeyCode::Backspace => {
                editor.backspace();
                self.current_tab_mut().is_pristine = false;
            }
            KeyCode::Delete => {
                editor.delete();
                self.current_tab_mut().is_pristine = false;
            }
            KeyCode::Enter => {
                editor.insert_newline();
                self.current_tab_mut().is_pristine = false;
            }
            KeyCode::Char(c) => {
                editor.insert_char(c);
                self.current_tab_mut().is_pristine = false;
            }
            _ => {}
        }
    }

    fn handle_table_key(&mut self, key: KeyEvent) {
        let table = &mut self.current_tab_mut().table;
        match key.code {
            KeyCode::Up => table.move_up(),
            KeyCode::Down => table.move_down(),
            KeyCode::Left => table.move_left(),
            KeyCode::Right => table.move_right(),
            KeyCode::PageUp => table.page_up(15),
            KeyCode::PageDown => table.page_down(15),
            KeyCode::Home => table.move_top(),
            KeyCode::End => table.move_bottom(),
            KeyCode::Char('s') => {
                let col = table.selected_col;
                table.toggle_sort(col);
                self.status_message = format!("Sorted by column {}", col + 1);
            }
            KeyCode::Char('c') => {
                if let Ok(text) = table.copy_current_cell() {
                    self.status_message = format!("📋 Copied cell: '{}'", text);
                }
            }
            KeyCode::Char('r') if table.copy_current_row().is_ok() => {
                self.status_message = "📋 Copied row to clipboard".to_string();
            }
            KeyCode::Char('a') if table.copy_entire_table().is_ok() => {
                self.status_message = format!(
                    "📋 Copied full table ({} rows) to clipboard",
                    table.row_count()
                );
            }
            _ => {}
        }
    }

    fn handle_modal_key(&mut self, key: KeyEvent) {
        match &mut self.modal {
            Modal::SaveQuery { name_input } => match key.code {
                KeyCode::Esc => self.modal = Modal::None,
                KeyCode::Enter => {
                    let name = name_input.trim().to_string();
                    if !name.is_empty() {
                        let sql = self.current_tab().editor.get_text();
                        let _ = save_query(&name, &sql);
                        self.config = load_config();
                        self.status_message = format!("💾 Query '{}' saved!", name);
                    }
                    self.modal = Modal::None;
                }
                KeyCode::Backspace => {
                    name_input.pop();
                }
                KeyCode::Char(c) => {
                    name_input.push(c);
                }
                _ => {}
            },
            Modal::ExportCsv { path_input } => match key.code {
                KeyCode::Esc => self.modal = Modal::None,
                KeyCode::Enter => {
                    let path = path_input.trim().to_string();
                    if !path.is_empty() {
                        match self.current_tab().table.export_csv(&path) {
                            Ok(rows) => {
                                self.status_message =
                                    format!("📥 Exported {} rows to '{}'", rows, path);
                            }
                            Err(e) => {
                                self.status_message = format!("❌ Export error: {}", e);
                            }
                        }
                    }
                    self.modal = Modal::None;
                }
                KeyCode::Backspace => {
                    path_input.pop();
                }
                KeyCode::Char(c) => {
                    path_input.push(c);
                }
                _ => {}
            },
            Modal::About | Modal::Help => {
                if key.code == KeyCode::Esc || key.code == KeyCode::Enter || key.code == KeyCode::Char('q') {
                    self.modal = Modal::None;
                }
            }
            Modal::None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_tabs_and_queries() {
        let mut app = App::new().expect("app init");
        assert_eq!(app.tabs.len(), 1);
        assert!(!app.current_tab().table.rows.is_empty());

        app.new_tab(Some("Tab 2"), Some("SELECT 100 AS num;"));
        assert_eq!(app.tabs.len(), 2);
        assert_eq!(app.active_tab, 1);

        app.run_query();
        assert_eq!(app.current_tab().table.rows[0][0], "100");

        app.close_active_tab();
        assert_eq!(app.tabs.len(), 1);
        assert_eq!(app.active_tab, 0);
    }
}
