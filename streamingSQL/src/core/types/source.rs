use futures::task::noop_waker_ref;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::sync::broadcast;
use tracing::info;
use tracing::warn;

use crate::pg_client::data::WalEvent;

#[derive(Debug)]
pub struct Source {
    receivers: HashMap<String, tokio::sync::broadcast::Receiver<Vec<WalEvent>>>,
    done: bool,
    watermark: usize,
}

impl Clone for Source {
    fn clone(&self) -> Self {
        Self {
            receivers: self
                .receivers
                .iter()
                .map(|(k, v)| (k.clone(), v.resubscribe()))
                .collect(),
            done: self.done,
            watermark: self.watermark,
        }
    }
}

impl Source {
    pub fn new() -> Self {
        Self {
            receivers: HashMap::new(),
            done: false,
            watermark: 0,
        }
    }

    pub fn insert(&mut self, table: String, rx: broadcast::Receiver<Vec<WalEvent>>) {
        self.receivers.insert(table, rx);
    }

    // Check if all receivers are done
    pub fn done(&self) -> bool {
        self.done
    }

    // Fetch a bounded amount of input changes from receivers
    pub fn fetch(&mut self) -> Option<Vec<(String, Vec<WalEvent>)>> {
        let mut results = Vec::new();

        // Iterate over each receiver
        for (table, rx) in self.receivers.iter_mut() {
            match rx.try_recv() {
                Ok(events) => {
                    results.push((table.clone(), events));
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    // No more events to receive
                    continue;
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    warn!("Receiver for table: {} is closed", table);
                    self.done = true;
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    warn!("Receiver for table: {} is lagged", table);
                }
            }
        }
        if results.is_empty() {
            return None;
        }
        return Some(results);
    }
}
