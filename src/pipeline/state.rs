use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FailedDept {
    pub dept: String,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BatchState {
    pub completed: HashSet<String>,
    pub failed: Vec<FailedDept>,
    pub started_at: String,
}

impl BatchState {
    pub fn new() -> Self {
        Self {
            completed: HashSet::new(),
            failed: Vec::new(),
            started_at: Utc::now().to_rfc3339(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let file = File::open(path).context("Failed to open state file")?;
        let reader = BufReader::new(file);
        let state = serde_json::from_reader(reader).context("Failed to parse state file")?;
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let file = File::create(path).context("Failed to create state file")?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self).context("Failed to write state file")?;
        Ok(())
    }

    pub fn mark_completed(&mut self, dept: &str) {
        self.completed.insert(dept.to_string());
        // Remove from failed if it was there?
        if let Some(pos) = self.failed.iter().position(|f| f.dept == dept) {
            self.failed.remove(pos);
        }
    }

    pub fn mark_failed(&mut self, dept: &str, err: String) {
        // Remove from completed?
        self.completed.remove(dept);

        if let Some(pos) = self.failed.iter().position(|f| f.dept == dept) {
            self.failed[pos].error = err;
        } else {
            self.failed.push(FailedDept {
                dept: dept.to_string(),
                error: err,
            });
        }
    }

    pub fn is_completed(&self, dept: &str) -> bool {
        self.completed.contains(dept)
    }
}
