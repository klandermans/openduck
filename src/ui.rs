use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Borders, Clear, List, ListItem, Paragraph, Row, Table, Tabs,
    },
    Frame,
};

use crate::app::{App, Focus, Modal, SidebarSection};

pub fn render(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Main body
            Constraint::Length(1), // Footer / Status bar
        ])
        .split(f.area());

    render_header(f, app, chunks[0]);
    render_body(f, app, chunks[1]);
    render_footer(f, app, chunks[2]);

    render_modal(f, app);
}

fn render_header(f: &mut Frame, app: &App, area: Rect) {
    let (dot, dot_color) = if app.status_message.contains("Error") {
        ("● Error", Color::Red)
    } else if app.status_message.contains("Running") {
        ("● Running", Color::Yellow)
    } else {
        ("● Ready", Color::Green)
    };

    let title_line = Line::from(vec![
        Span::styled("🦆 ", Style::default()),
        Span::styled("OpenDuck ", Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan)),
        Span::styled("• Flat File SQL Client [DuckDB + Ratatui] ", Style::default().fg(Color::DarkGray)),
        Span::styled(dot, Style::default().fg(dot_color).add_modifier(Modifier::BOLD)),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let p = Paragraph::new(title_line).block(block);
    f.render_widget(p, area);
}

fn render_body(f: &mut Frame, app: &mut App, area: Rect) {
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(32), // Sidebar
            Constraint::Min(40),    // Main content (Editor + Results)
        ])
        .split(area);

    render_sidebar(f, app, main_chunks[0]);
    render_content(f, app, main_chunks[1]);
}

fn render_sidebar(f: &mut Frame, app: &mut App, area: Rect) {
    let sidebar_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(50), // Files
            Constraint::Percentage(25), // Saved Queries
            Constraint::Percentage(25), // History
        ])
        .split(area);

    let is_focused = app.focus == Focus::Sidebar;

    // 1. Files Section
    let file_border_style = if is_focused && app.sidebar_section == SidebarSection::Files {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let files_title = format!(
        " [1] Files ({}/{}) ",
        if app.file_tree.items.is_empty() { 0 } else { app.file_tree.selected_index + 1 },
        app.file_tree.items.len()
    );

    let files_block = Block::default()
        .title(files_title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(file_border_style);

    let inner_files = files_block.inner(sidebar_chunks[0]);
    let f_height = inner_files.height as usize;
    let f_selected = app.file_tree.selected_index;
    let f_scroll = if f_selected < app.file_tree.scroll_offset {
        f_selected
    } else if f_height > 0 && f_selected >= app.file_tree.scroll_offset + f_height {
        f_selected + 1 - f_height
    } else {
        app.file_tree.scroll_offset
    };
    app.file_tree.scroll_offset = f_scroll;
    let f_start = f_scroll.min(app.file_tree.items.len());
    let f_end = (f_start + f_height).min(app.file_tree.items.len());

    let file_items: Vec<ListItem> = app.file_tree.items[f_start..f_end]
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let actual_idx = f_start + i;
            let selected = actual_idx == f_selected;
            let (icon, style) = if item.is_dir {
                ("📁 ", Style::default().fg(Color::Blue))
            } else if item.is_supported {
                ("🦆 ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
            } else {
                ("   ", Style::default().fg(Color::DarkGray))
            };

            let item_style = if selected && is_focused && app.sidebar_section == SidebarSection::Files {
                style.bg(Color::Rgb(30, 45, 60)).add_modifier(Modifier::BOLD)
            } else {
                style
            };

            let prefix = if selected { "▶ " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::raw(prefix),
                Span::raw(icon),
                Span::styled(&item.name, item_style),
            ]))
        })
        .collect();

    let files_list = List::new(file_items).block(files_block);
    f.render_widget(files_list, sidebar_chunks[0]);

    // 2. Saved Queries Section
    let saved_border_style = if is_focused && app.sidebar_section == SidebarSection::SavedQueries {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let saved_title = format!(
        " [2] Saved Queries ({}/{}) ",
        if app.config.saved_queries.is_empty() { 0 } else { app.saved_queries_index + 1 },
        app.config.saved_queries.len()
    );

    let saved_block = Block::default()
        .title(saved_title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(saved_border_style);

    let inner_saved = saved_block.inner(sidebar_chunks[1]);
    let s_height = inner_saved.height as usize;
    let s_selected = app.saved_queries_index;
    let s_scroll = if s_selected < app.saved_queries_scroll {
        s_selected
    } else if s_height > 0 && s_selected >= app.saved_queries_scroll + s_height {
        s_selected + 1 - s_height
    } else {
        app.saved_queries_scroll
    };
    app.saved_queries_scroll = s_scroll;
    let s_start = s_scroll.min(app.config.saved_queries.len());
    let s_end = (s_start + s_height).min(app.config.saved_queries.len());

    let saved_items: Vec<ListItem> = app.config.saved_queries[s_start..s_end]
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let actual_idx = s_start + i;
            let selected = actual_idx == s_selected;
            let style = if selected && is_focused && app.sidebar_section == SidebarSection::SavedQueries {
                Style::default().fg(Color::Magenta).bg(Color::Rgb(40, 30, 50)).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            let prefix = if selected { "▶ " } else { "  " };
            ListItem::new(Line::from(vec![
                Span::raw(prefix),
                Span::styled(&q.name, style),
            ]))
        })
        .collect();

    let saved_list = List::new(saved_items).block(saved_block);
    f.render_widget(saved_list, sidebar_chunks[1]);

    // 3. History Section
    let history_border_style = if is_focused && app.sidebar_section == SidebarSection::History {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let history_title = format!(
        " [3] History ({}/{}) ",
        if app.config.history.is_empty() { 0 } else { app.history_index + 1 },
        app.config.history.len()
    );

    let history_block = Block::default()
        .title(history_title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(history_border_style);

    let inner_hist = history_block.inner(sidebar_chunks[2]);
    let h_height = inner_hist.height as usize;
    let h_selected = app.history_index;
    let h_scroll = if h_selected < app.history_scroll {
        h_selected
    } else if h_height > 0 && h_selected >= app.history_scroll + h_height {
        h_selected + 1 - h_height
    } else {
        app.history_scroll
    };
    app.history_scroll = h_scroll;

    let history_reversed: Vec<_> = app.config.history.iter().rev().collect();
    let h_start = h_scroll.min(history_reversed.len());
    let h_end = (h_start + h_height).min(history_reversed.len());

    let history_items: Vec<ListItem> = history_reversed[h_start..h_end]
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let actual_idx = h_start + i;
            let selected = actual_idx == h_selected;
            let style = if selected && is_focused && app.sidebar_section == SidebarSection::History {
                Style::default().fg(Color::Cyan).bg(Color::Rgb(30, 45, 60)).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let prefix = if selected { "▶ " } else { "  " };
            let first_line = h.sql.lines().next().unwrap_or("").trim();
            ListItem::new(Line::from(vec![
                Span::raw(prefix),
                Span::styled(first_line, style),
            ]))
        })
        .collect();

    let history_list = List::new(history_items).block(history_block);
    f.render_widget(history_list, sidebar_chunks[2]);
}

fn render_content(f: &mut Frame, app: &mut App, area: Rect) {
    let content_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Tab bar
            Constraint::Length(8), // SQL Editor
            Constraint::Length(1), // Actions toolbar
            Constraint::Min(6),    // Results Table
        ])
        .split(area);

    render_tabs(f, app, content_chunks[0]);
    render_editor(f, app, content_chunks[1]);
    render_toolbar(f, app, content_chunks[2]);
    render_table(f, app, content_chunks[3]);
}

fn render_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = app
        .tabs
        .iter()
        .enumerate()
        .map(|(i, tab)| {
            let is_active = i == app.active_tab;
            let style = if is_active {
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let title = format!(" {} [✕] ", tab.title);
            Line::from(vec![Span::styled(title, style)])
        })
        .collect();

    let tabs_widget = Tabs::new(titles)
        .select(app.active_tab)
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .divider(Span::styled(" | ", Style::default().fg(Color::DarkGray)));

    f.render_widget(tabs_widget, area);
}

fn render_editor(f: &mut Frame, app: &App, area: Rect) {
    let tab = app.current_tab();
    let is_focused = app.focus == Focus::Editor;

    let border_style = if is_focused {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let title = format!(
        " SQL Editor [Line {}, Col {}] (Ctrl+Enter to run) ",
        tab.editor.cursor_row + 1,
        tab.editor.cursor_col + 1
    );

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    // Calculate viewable lines based on inner_area height
    let height = inner_area.height as usize;
    let cursor_row = tab.editor.cursor_row;
    let cursor_col = tab.editor.cursor_col;

    let start_row = if cursor_row >= height {
        cursor_row.saturating_sub(height - 1)
    } else {
        0
    };

    let line_no_width = 6usize;
    let code_width = (inner_area.width as usize).saturating_sub(line_no_width).max(1);
    let col_offset = if cursor_col >= code_width {
        cursor_col + 1 - code_width
    } else {
        0
    };

    let mut lines = Vec::new();
    for row_idx in start_row..tab.editor.lines.len().min(start_row + height) {
        let line_text = &tab.editor.lines[row_idx];
        let is_current_line = is_focused && row_idx == cursor_row;

        let num_span = Span::styled(
            format!("{:3} │ ", row_idx + 1),
            Style::default().fg(Color::DarkGray),
        );

        let mut spans = vec![num_span];
        let chars: Vec<char> = line_text.chars().collect();

        if is_current_line {
            let vis_cursor_col = cursor_col.saturating_sub(col_offset);
            let window: Vec<char> = if col_offset < chars.len() {
                chars[col_offset..chars.len().min(col_offset + code_width)].to_vec()
            } else {
                Vec::new()
            };

            let before: String = window[..vis_cursor_col.min(window.len())].iter().collect();
            let cursor_char = if vis_cursor_col < window.len() {
                window[vis_cursor_col].to_string()
            } else {
                " ".to_string()
            };
            let after: String = if vis_cursor_col + 1 < window.len() {
                window[vis_cursor_col + 1..].iter().collect()
            } else {
                String::new()
            };

            spans.push(format_sql_span(&before));
            spans.push(Span::styled(
                cursor_char,
                Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD),
            ));
            spans.push(format_sql_span(&after));
        } else {
            let window: String = if col_offset < chars.len() {
                chars[col_offset..chars.len().min(col_offset + code_width)].iter().collect()
            } else {
                String::new()
            };
            spans.push(format_sql_span(&window));
        }

        lines.push(Line::from(spans));
    }

    let editor_paragraph = Paragraph::new(lines);
    f.render_widget(editor_paragraph, inner_area);
}

fn format_sql_span(text: &str) -> Span<'static> {
    let trimmed = text.trim();
    if trimmed.starts_with("--") {
        return Span::styled(text.to_string(), Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC));
    }
    Span::styled(text.to_string(), Style::default().fg(Color::White))
}

fn render_toolbar(f: &mut Frame, _app: &App, area: Rect) {
    let toolbar_line = Line::from(vec![
        Span::styled(" [▶ Run (Ctrl+Enter)] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::styled(" [💬 Comment (F3)] ", Style::default().fg(Color::Cyan)),
        Span::styled(" [💾 Save (Ctrl+S)] ", Style::default().fg(Color::Magenta)),
        Span::styled(" [📥 Export (Ctrl+E)] ", Style::default().fg(Color::Yellow)),
        Span::styled(" [📋 Copy (c/r/a)] ", Style::default().fg(Color::White)),
        Span::styled(" [❓ Help (F2)] ", Style::default().fg(Color::LightBlue)),
    ]);
    let p = Paragraph::new(toolbar_line);
    f.render_widget(p, area);
}

fn render_table(f: &mut Frame, app: &mut App, area: Rect) {
    let is_focused = app.focus == Focus::Table;
    let tab = app.current_tab_mut();

    let border_style = if is_focused {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let title = format!(
        " Results Table [{} rows, {} cols] {} ",
        tab.table.row_count(),
        tab.table.col_count(),
        if is_focused {
            format!(
                "(Row {}/{}, Col {}/{}) [Arrows: nav, s: sort, c: cell, r: row, a: tsv]",
                if tab.table.rows.is_empty() { 0 } else { tab.table.selected_row + 1 },
                tab.table.row_count(),
                if tab.table.columns.is_empty() { 0 } else { tab.table.selected_col + 1 },
                tab.table.col_count()
            )
        } else {
            String::new()
        }
    );

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border_style);

    let inner_area = block.inner(area);
    f.render_widget(block, area);

    if let Some(err) = &tab.error {
        let err_paragraph = Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(err_paragraph, inner_area);
        return;
    }

    if tab.table.columns.is_empty() {
        let empty_msg = Paragraph::new("No results to display. Press Ctrl+Enter to run query.")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(empty_msg, inner_area);
        return;
    }

    let visible_height = inner_area.height.saturating_sub(2) as usize;
    let col_width = 16u16;

    let (start_row, end_row, start_col, end_col) =
        tab.table.visible_slice(visible_height, col_width, inner_area.width);

    tab.table.scroll_row = start_row;
    tab.table.scroll_col = start_col;

    // Prepare table headers for visible slice
    let header_cells = tab.table.columns[start_col..end_col].iter().enumerate().map(|(idx, col)| {
        let actual_col_idx = start_col + idx;
        let sort_icon = if tab.table.sort_col == Some(actual_col_idx) {
            if tab.table.sort_desc { " ▼" } else { " ▲" }
        } else {
            ""
        };
        let text = format!("{}{}", col, sort_icon);
        let style = if tab.table.sort_col == Some(actual_col_idx) {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
        };
        Span::styled(text, style)
    });
    let header_row = Row::new(header_cells).height(1).bottom_margin(1);

    let selected_row = tab.table.selected_row;
    let selected_col = tab.table.selected_col;

    let table_rows = tab.table.rows[start_row..end_row]
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let actual_row_idx = start_row + idx;
            let is_row_selected = is_focused && actual_row_idx == selected_row;

            let visible_row_cells = if start_col < row.len() {
                let row_end = end_col.min(row.len());
                &row[start_col..row_end]
            } else {
                &[]
            };

            let cells = visible_row_cells.iter().enumerate().map(|(c_idx, val)| {
                let actual_col_idx = start_col + c_idx;
                let is_cell_selected = is_row_selected && actual_col_idx == selected_col;
                if is_cell_selected {
                    Span::styled(
                        val.clone(),
                        Style::default().fg(Color::Black).bg(Color::Yellow).add_modifier(Modifier::BOLD),
                    )
                } else if is_row_selected {
                    Span::styled(
                        val.clone(),
                        Style::default().fg(Color::White).bg(Color::Rgb(40, 50, 70)),
                    )
                } else if actual_row_idx % 2 == 0 {
                    Span::styled(
                        val.clone(),
                        Style::default().fg(Color::White).bg(Color::Rgb(20, 20, 25)),
                    )
                } else {
                    Span::styled(val.clone(), Style::default().fg(Color::White))
                }
            });

            Row::new(cells).height(1)
        });

    let rendered_col_count = end_col.saturating_sub(start_col).max(1);
    let dynamic_col_width = (inner_area.width / rendered_col_count as u16).max(col_width);
    let constraints: Vec<Constraint> = (0..rendered_col_count)
        .map(|_| Constraint::Length(dynamic_col_width))
        .collect();

    let table_widget = Table::new(table_rows, constraints)
        .header(header_row)
        .block(Block::default().borders(Borders::NONE));

    f.render_widget(table_widget, inner_area);
}

fn render_footer(f: &mut Frame, app: &App, area: Rect) {
    let tab = app.current_tab();
    let text = format!(
        " {} | Duration: {}ms | Tab: Switch Pane | F1: About | F2: Help | Ctrl+Q: Quit",
        app.status_message, tab.duration_ms
    );
    let p = Paragraph::new(text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(p, area);
}

fn render_modal(f: &mut Frame, app: &App) {
    match &app.modal {
        Modal::None => {}
        Modal::SaveQuery { name_input } => {
            let area = centered_rect(50, 20, f.area());
            f.render_widget(Clear, area);

            let block = Block::default()
                .title(" 💾 Save Query ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Magenta));

            let inner = block.inner(area);
            f.render_widget(block, area);

            let lines = vec![
                Line::from(vec![Span::raw("Enter a name for this query:")]),
                Line::from(vec![]),
                Line::from(vec![
                    Span::styled("Name: ", Style::default().fg(Color::Cyan)),
                    Span::styled(name_input, Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                    Span::styled("█", Style::default().fg(Color::Yellow)),
                ]),
                Line::from(vec![]),
                Line::from(vec![
                    Span::styled("[Enter: Save]  [Esc: Cancel]", Style::default().fg(Color::DarkGray)),
                ]),
            ];

            let p = Paragraph::new(lines);
            f.render_widget(p, inner);
        }
        Modal::ExportCsv { path_input } => {
            let area = centered_rect(50, 20, f.area());
            f.render_widget(Clear, area);

            let block = Block::default()
                .title(" 📥 Export Results to CSV ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Yellow));

            let inner = block.inner(area);
            f.render_widget(block, area);

            let lines = vec![
                Line::from(vec![Span::raw("Enter export destination CSV file path:")]),
                Line::from(vec![]),
                Line::from(vec![
                    Span::styled("Path: ", Style::default().fg(Color::Cyan)),
                    Span::styled(path_input, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                    Span::styled("█", Style::default().fg(Color::Yellow)),
                ]),
                Line::from(vec![]),
                Line::from(vec![
                    Span::styled("[Enter: Export]  [Esc: Cancel]", Style::default().fg(Color::DarkGray)),
                ]),
            ];

            let p = Paragraph::new(lines);
            f.render_widget(p, inner);
        }
        Modal::About => {
            let area = centered_rect(60, 50, f.area());
            f.render_widget(Clear, area);

            let block = Block::default()
                .title(" 🦆 About OpenDuck ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Cyan));

            let inner = block.inner(area);
            f.render_widget(block, area);

            let lines = vec![
                Line::from(Span::styled("     __", Style::default().fg(Color::Yellow))),
                Line::from(Span::styled("   <(o )___", Style::default().fg(Color::Yellow))),
                Line::from(Span::styled("    ( ._> /", Style::default().fg(Color::Yellow))),
                Line::from(Span::styled("     `---'", Style::default().fg(Color::Yellow))),
                Line::from(vec![]),
                Line::from(Span::styled("🦆 OpenDuck v0.2.0 (Rust Edition)", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
                Line::from(Span::styled("A high-performance flat file SQL client based on DuckDB.", Style::default().fg(Color::White))),
                Line::from(vec![]),
                Line::from(Span::styled("Powered by Ratatui (https://ratatui.rs) & DuckDB.", Style::default().fg(Color::Green))),
                Line::from(Span::styled("Direct support for Parquet, CSV, JSON, Arrow, SQLite & DuckDB.", Style::default().fg(Color::DarkGray))),
                Line::from(vec![]),
                Line::from(Span::styled("Press [Enter] or [Esc] to close.", Style::default().fg(Color::DarkGray))),
            ];

            let p = Paragraph::new(lines).alignment(Alignment::Center);
            f.render_widget(p, inner);
        }
        Modal::Help => {
            let area = centered_rect(65, 65, f.area());
            f.render_widget(Clear, area);

            let block = Block::default()
                .title(" ⌨️ OpenDuck Shortcuts ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Green));

            let inner = block.inner(area);
            f.render_widget(block, area);

            let lines = vec![
                Line::from(Span::styled("Navigation & Windows:", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
                Line::from("  Tab / Shift+Tab   : Switch focus (Sidebar ↔ Editor ↔ Table)"),
                Line::from("  Alt+Left / Right  : Previous / Next Tab"),
                Line::from("  Ctrl+T / Ctrl+W   : New Tab / Close Tab"),
                Line::from(vec![]),
                Line::from(Span::styled("Queries & Editing:", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
                Line::from("  Ctrl+Enter / F5   : Run current query"),
                Line::from("  Ctrl+S            : Save named query to openduck.json"),
                Line::from("  F3 / Ctrl+/       : Toggle line comment (-- )"),
                Line::from("  Ctrl+Z / Ctrl+Y   : Undo / Redo"),
                Line::from(vec![]),
                Line::from(Span::styled("Results Table:", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
                Line::from("  Arrow keys        : Navigate cells & rows"),
                Line::from("  's'               : Toggle sorting on selected column"),
                Line::from("  'c'               : Copy cell to clipboard"),
                Line::from("  'r'               : Copy row to clipboard"),
                Line::from("  'a'               : Copy entire table (TSV) to clipboard"),
                Line::from("  Ctrl+E            : Export results to CSV"),
                Line::from(vec![]),
                Line::from(Span::styled("Press [Enter] or [Esc] to close.", Style::default().fg(Color::DarkGray))),
            ];

            let p = Paragraph::new(lines);
            f.render_widget(p, inner);
        }
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
