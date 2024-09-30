use abomonation::{unsafe_abomonate, Abomonation};
use core::hash::Hash;
use core::{fmt::Debug, panic};
use serde_json::{Number, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::Hasher;

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
                        match change_event.pkey.val {
                            Value::Number(num) => usize::try_from(num.as_u64().unwrap()).unwrap(),
                            Value::String(str) => {
                                let mut hasher = DefaultHasher::new();
                                str.hash(&mut hasher);
                                let hash = hasher.finish();
                                usize::try_from(hash).unwrap()
                            }
                            _ => {
                                panic!("Failed to parse primary key")
                            }
                        },
                        match insert {
                            Insert(data) => DBRecord(data),
                            _ => DBRecord::new(),
                        },
                    ),
                    time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                    change: 1,
                }),
                WalData::Update(update) => {
                    input.push(DataflowInput {
                        element: DataflowData(
                            change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                            DBRecord::new(),
                        ),
                        time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                        change: -1,
                    });
                    input.push(DataflowInput {
                        element: DataflowData(
                            change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                            match update {
                                Update(data) => DBRecord(data),
                                _ => DBRecord::new(),
                            },
                        ),
                        time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                        change: 1,
                    });
                }
                WalData::Delete => input.push(DataflowInput {
                    element: DataflowData(
                        change_event.pkey.val.as_str().unwrap().parse().unwrap(),
                        DBRecord::new(),
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
pub struct DataflowData(pub usize, pub DBRecord);

#[derive(Clone, Debug)]

pub struct DBRecord(pub BTreeMap<String, Value>);

impl DBRecord {
    pub fn new() -> Self {
        DBRecord(BTreeMap::new())
    }
    pub fn as_raw_pointer(&self) -> *const DBRecord {
        let raw = Box::new(self.clone());
        Box::into_raw(raw) as *const DBRecord
    }
    pub fn from_raw_pointer(raw: *const DBRecord) -> Self {
        unsafe { (*(*&raw as *const DBRecord)).clone() }
    }
}

impl PartialEq for DataflowData {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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
