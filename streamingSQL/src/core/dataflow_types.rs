use abomonation::{unsafe_abomonate, Abomonation};
use core::fmt::Debug;
use core::hash::Hash;
use differential_dataflow::{Data, ExchangeData};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::pg_client::data::{Insert, Update, WalData, WalEvent};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataflowInput {
    pub element: DataflowData,
    pub time: usize,
    pub change: isize,
}

impl DataflowInput {
    pub fn from_wal_event(event: Vec<WalEvent>) -> Vec<Self> {
        let mut input = Vec::new();
        for change_event in event {
            match change_event.data {
                WalData::Insert(insert) => input.push(DataflowInput {
                    element: DataflowData(
                        change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                        match insert {
                            Insert(data) => (data),
                            _ => BTreeMap::new(),
                        },
                    ),
                    time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                    change: 1,
                }),
                WalData::Update(update) => {
                    input.push(DataflowInput {
                        element: DataflowData(
                            change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                            BTreeMap::new(),
                        ),
                        time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                        change: -1,
                    });
                    input.push(DataflowInput {
                        element: DataflowData(
                            change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                            match update {
                                Update(data) => (data),
                                _ => BTreeMap::new(),
                            },
                        ),
                        time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                        change: 1,
                    });
                }
                WalData::Delete => input.push(DataflowInput {
                    element: DataflowData(
                        change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                        BTreeMap::new(),
                    ),
                    time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                    change: -1,
                }),
            }
        }
        return input;
    }
}

unsafe_abomonate!(DataflowData);
#[derive(Clone, Debug)]
pub struct DataflowData(pub usize, pub BTreeMap<String, Value>);

impl PartialEq for DataflowData {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl Hash for DataflowData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Eq for DataflowData {}

impl PartialOrd for DataflowData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for DataflowData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
