#[derive(Debug)]
pub struct History {
    entries: Vec<String>,
    offset: usize,
}

impl History {
    pub fn new() -> Self {
        History {
            entries: Vec::new(),
            offset: 0,
        }
    }

    pub fn push(&mut self, entry: String) {
        if self.entries.last() != Some(&entry.clone()) {
            self.entries.push(entry);
        }

        self.offset = self.entries.len();
    }

    pub fn prev_entry(&mut self) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }

        if self.offset == 1 && self.entries.len() == 1 {
            return self.entries.first().cloned();
        }

        if self.offset >= 1 {
            self.offset -= 1;
        }

        Some(
            self.entries
                .get(self.offset)
                .cloned()
                .expect("My maintainer miscalculated the history offset for prev_entry"),
        )
    }

    pub fn next_entry(&mut self) -> Option<String> {
        if self.entries.is_empty() || self.offset == self.entries.len() {
            return Some("".to_string());
        }

        self.offset += 1;

        if self.offset == self.entries.len() {
            return Some("".to_string());
        }

        Some(
            self.entries
                .get(self.offset)
                .cloned()
                .expect("My maintainer miscalculated the history offset for next_entry"),
        )
    }
}
