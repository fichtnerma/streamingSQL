use std::collections::HashMap;

use differential_dataflow::{input::InputSession, Collection};
use timely::worker::Worker;
use tracing::info;

use super::dataflow_types::DataflowData;

pub struct InputSessions(HashMap<String, InputSession<usize, DataflowData, isize>>);

impl InputSessions {
    pub fn new(tables: Vec<String>) -> Self {
        let mut inputs = HashMap::new();
        for table in tables {
            let input: InputSession<usize, DataflowData, isize> = InputSession::new();
            inputs.insert(table, input);
        }
        info!("InputSessions created");
        return InputSessions(inputs);
    }

    pub fn insert(&mut self, key: String, value: InputSession<usize, DataflowData, isize>) {
        self.0.insert(key, value);
    }

    pub fn get(&mut self, key: &str) -> Option<&mut InputSession<usize, DataflowData, isize>> {
        info!("Getting input session for table: {}", key);
        info!("InputSessions: {:?}", self.0.keys());
        self.0.get_mut(key)
    }

    pub fn advance_to(&mut self, time: usize) {
        for input in self.0.values_mut() {
            input.advance_to(time);
            input.time();
        }
    }

    pub fn time(&mut self) -> usize {
        return *self.0.values_mut().last().unwrap().time();
    }

    pub fn flush(&mut self) {
        for input in self.0.values_mut() {
            input.flush();
        }
    }

    pub fn update_at_for_table(
        &mut self,
        table: &str,
        element: DataflowData,
        time: usize,
        change: isize,
    ) {
        if let Some(input) = self.0.get_mut(table) {
            input.update_at(element, time, change);
        }
    }
}

struct Arrangements(HashMap<String, InputSession<usize, DataflowData, isize>>);
