use std::{
    collections::VecDeque,
    env,
    time::{Duration, Instant},
    u16,
};

use super::dataflow_types::DataflowData;

pub struct Buffer {
    pub data: VecDeque<(String, DataflowData, usize, isize)>,
    pub watermark: usize,
    pub size: u16,  // Size of the buffer after which it should pop
    pub delay: u32, // Delay in milliseconds
    last_pop: Instant,
}

impl Buffer {
    pub fn new() -> Self {
        let ev = |name| env::var(name).unwrap();
        Self {
            data: VecDeque::new(),
            watermark: 0,
            size: ev("BUFFER_SIZE").parse().unwrap(),
            delay: ev("BUFFER_DELAY").parse().unwrap(),
            last_pop: Instant::now(),
        }
    }

    pub fn insert(&mut self, table: String, data: DataflowData, time: usize, change: isize) {
        self.data.push_back((table, data, time, change));
    }

    pub fn get_watermark(&self) -> usize {
        self.watermark
    }

    fn should_pop(&self) -> bool {
        if self.data.is_empty() {
            return false;
        }

        // Check if the buffer is full
        if self.data.len() as u16 >= self.size * 2 {
            return true;
        }

        // Check if the delay has passed since the last pop
        let elapsed_time = self.last_pop.elapsed();
        if elapsed_time >= Duration::from_millis(self.delay as u64) {
            return true;
        }

        false
    }

    pub fn update_watermark(&mut self, time: usize) {
        self.watermark = time;
    }

    pub fn pop(&mut self) -> Option<Vec<(String, DataflowData, usize, isize)>> {
        if self.should_pop() {
            let mut results = Vec::new();
            self.data
                .iter()
                .collect::<Vec<_>>()
                .sort_by(|a, b| a.2.cmp(&b.2));
            while let Some((table, data, time, change)) = self.data.pop_front() {
                results.push((table, data, time, change));
                if results.len() as u16 >= self.size && self.data.front().unwrap().2 != time {
                    break;
                }
            }
            self.last_pop = Instant::now(); // Update the last pop time
            return Some(results);
        }
        return None;
    }
}
