use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::is_duckdb_extension;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileItem {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub is_supported: bool,
}

#[derive(Debug, Clone)]
pub struct FileTree {
    pub current_dir: PathBuf,
    pub items: Vec<FileItem>,
    pub selected_index: usize,
    pub scroll_offset: usize,
}

impl Default for FileTree {
    fn default() -> Self {
        Self::new(PathBuf::from("."))
    }
}

impl FileTree {
    pub fn new(dir: PathBuf) -> Self {
        let mut tree = Self {
            current_dir: dir,
            items: Vec::new(),
            selected_index: 0,
            scroll_offset: 0,
        };
        let _ = tree.refresh();
        tree
    }

    pub fn refresh(&mut self) -> Result<()> {
        let mut dirs = Vec::new();
        let mut files = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.current_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();

                // Skip hidden files/dirs like .git, .DS_Store
                if file_name.starts_with('.') && file_name != ".." {
                    continue;
                }

                if path.is_dir() {
                    dirs.push(FileItem {
                        name: file_name,
                        path,
                        is_dir: true,
                        is_supported: false,
                    });
                } else if path.is_file() {
                    let is_supported = is_duckdb_extension(&file_name);
                    files.push(FileItem {
                        name: file_name,
                        path,
                        is_dir: false,
                        is_supported,
                    });
                }
            }
        }

        dirs.sort_by_key(|a| a.name.to_lowercase());
        files.sort_by_key(|a| a.name.to_lowercase());

        let mut all_items = Vec::new();
        // If not at root, add parent directory ".."
        if self.current_dir.parent().is_some() && self.current_dir != Path::new(".") {
            all_items.push(FileItem {
                name: "..".to_string(),
                path: self.current_dir.join(".."),
                is_dir: true,
                is_supported: false,
            });
        }
        all_items.extend(dirs);
        all_items.extend(files);

        self.items = all_items;
        if self.selected_index >= self.items.len() {
            self.selected_index = self.items.len().saturating_sub(1);
        }
        Ok(())
    }

    pub fn move_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    pub fn move_down(&mut self) {
        if !self.items.is_empty() && self.selected_index + 1 < self.items.len() {
            self.selected_index += 1;
        }
    }

    pub fn selected_item(&self) -> Option<&FileItem> {
        self.items.get(self.selected_index)
    }

    pub fn enter_selected(&mut self) -> Option<PathBuf> {
        let item = self.selected_item()?.clone();
        if item.is_dir {
            if let Ok(canonical) = fs::canonicalize(&item.path) {
                self.current_dir = canonical;
            } else {
                self.current_dir = item.path;
            }
            self.selected_index = 0;
            self.scroll_offset = 0;
            let _ = self.refresh();
            None
        } else {
            Some(item.path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_tree_lists_supported_files() {
        let tree = FileTree::new(PathBuf::from("."));
        assert!(!tree.items.is_empty());
        // There should be some dirs or files
        let has_samples = tree.items.iter().any(|i| i.name == "samples");
        assert!(has_samples);
    }
}
