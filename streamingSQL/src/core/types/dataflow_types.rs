use abomonation::{unsafe_abomonate, Abomonation};
use arrayvec::ArrayString;
use core::hash::Hash;
use core::{fmt::Debug, panic};
use serde_json::{Number, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};

use crate::pg_client::data::{Insert, Update, WalData, WalEvent};

unsafe_abomonate!(AbomonationWrapper<ArrayString<25>>);
unsafe_abomonate!(AbomonationWrapper<ArrayString<40>>);
unsafe_abomonate!(AbomonationWrapper<ArrayString<128>>);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug, Hash, Default)]
pub struct AbomonationWrapper<T> {
    pub element: T,
}

use ::std::ops::Deref;
impl<T> Deref for AbomonationWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.element
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataflowInput {
    pub element: DataflowData,
    pub time: usize,
    pub change: isize,
}

impl DataflowInput {
    pub fn from_wal_event(event: Vec<WalEvent>, required_key: Option<String>) -> Vec<Self> {
        let mut input = Vec::new();
        for change_event in event {
            match change_event.clone().data {
                WalData::Insert(insert) => {
                    let keys = get_required_key(required_key.clone(), change_event.clone());
                    input.push(DataflowInput {
                        element: DataflowData(
                            keys.primary,
                            match insert {
                                Insert(data) => (keys.foreign, DBRecord(data)),
                                _ => (keys.foreign, DBRecord::new()),
                            },
                        ),
                        time: usize::try_from(change_event.xid.clone())
                            .expect("Failed to convert time"),
                        change: 1,
                    })
                }
                WalData::Update(update) => {
                    let keys = get_required_key(required_key.clone(), change_event.clone());
                    input.push(DataflowInput {
                        element: DataflowData(
                            keys.clone().primary,
                            (keys.foreign, DBRecord::new()),
                        ),
                        time: usize::try_from(change_event.xid.clone() - 1)
                            .expect("Failed to convert time"),
                        change: -1,
                    });
                    input.push(DataflowInput {
                        element: DataflowData(
                            keys.primary,
                            match update {
                                Update(data) => (keys.foreign, DBRecord(data)),
                                _ => (keys.foreign, DBRecord::new()),
                            },
                        ),
                        time: usize::try_from(change_event.xid).expect("Failed to convert time"),
                        change: 1,
                    });
                    debug!("Update event: {:?}", input);
                }
                WalData::Delete => {
                    debug!("Delete event: {:?}", change_event);
                    let keys = get_required_key(required_key.clone(), change_event.clone());

                    input.push(DataflowInput {
                        element: DataflowData(keys.primary, (keys.foreign, DBRecord::new())),
                        time: usize::try_from(change_event.xid.clone())
                            .expect("Failed to convert time"),
                        change: -1,
                    })
                }
            }
        }
        return input;
    }
}
fn get_required_key(required_key: Option<String>, change_event: WalEvent) -> Keys {
    let prim_key_val = change_event.pkey.val;
    let foreign_key_val = match required_key {
        Some(key) => {
            let data = match change_event.data {
                WalData::Insert(insert) => match insert {
                    Insert(data) => data,
                    _ => panic!("Failed to parse insert data"),
                },
                WalData::Update(update) => match update {
                    Update(data) => data,
                    _ => panic!("Failed to parse update data"),
                },
                WalData::Delete => BTreeMap::new(),
            };
            let key = match data.get(&key) {
                Some(val) => val.clone(),
                None => Value::Null,
            };
            key
        }
        None => Value::Null,
    };
    Keys {
        primary: key_to_usize(prim_key_val),
        foreign: match foreign_key_val {
            Value::Null => None,
            _ => Some(key_to_usize(foreign_key_val)),
        },
    }
}

fn key_to_usize(value: Value) -> usize {
    match value {
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
    }
}

#[derive(Clone, Debug)]
pub struct DataflowData(pub usize, pub (Option<usize>, DBRecord));

impl IntoIterator for DataflowData {
    type Item = usize;
    type IntoIter = std::vec::IntoIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        let mut vec = Vec::new();
        vec.push(self.0);
        match self.1 {
            (Some(foreign), _) => vec.push(foreign),
            _ => {}
        }
        vec.into_iter()
    }
}

unsafe_abomonate!(Keys);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keys {
    pub primary: usize,
    pub foreign: Option<usize>,
}

impl Keys {
    pub fn swap(self) -> Self {
        let mut clone = self.clone();
        let temp = clone.primary;
        clone.primary = clone.foreign.unwrap();
        clone.foreign = Some(temp);
        clone
    }
}

#[derive(Clone, Debug)]
pub enum RecordType {
    Insert = 1,
    Delete = -1,
}

impl RecordType {
    pub fn from_value(value: isize) -> RecordType {
        match value {
            1 => RecordType::Insert,
            -1 => RecordType::Delete,
            _ => panic!("Invalid RecordType"),
        }
    }
}

unsafe_abomonate!(DBRecord);
#[derive(Clone, Debug)]
pub struct DBRecord(pub BTreeMap<String, Value>);

impl DBRecord {
    pub fn new() -> Self {
        DBRecord(BTreeMap::new())
    }
    pub fn get(&self, key: &str) -> Value {
        match self.0.get(key) {
            Some(value) => value.clone(),
            None => Value::Null,
        }
    }
    pub fn get_key_for_value(&self, value: usize) -> Option<String> {
        for (key, val) in self.0.iter() {
            if val == &value {
                return Some(key.clone());
            }
        }
        None
    }
    pub fn as_raw_pointer(&self) -> usize {
        let raw = Box::new(self.clone());
        Box::into_raw(raw) as *const DBRecord as usize
    }
    pub fn to_sql_values(
        &self,
        record_type: RecordType,
        table: String,
        keys: Option<Vec<(String, usize)>>,
    ) -> String {
        match record_type {
            RecordType::Insert => {
                if self.0.is_empty() {
                    return "".to_string();
                }
                let mut sql_string = format!("INSERT INTO {} VALUES (", table);
                for (_, value) in self.0.iter() {
                    match value {
                        Value::Number(num) => {
                            sql_string.push_str(&format!(" {},", num));
                        }
                        Value::String(str) => {
                            sql_string.push_str(&format!(" '{}',", str));
                        }
                        Value::Bool(bool) => {
                            sql_string.push_str(&format!(" {},", bool));
                        }
                        _ => {}
                    }
                }
                sql_string.pop();
                sql_string.push_str(");");
                return sql_string;
            }
            RecordType::Delete => {
                let mut sql_string = format!("DELETE FROM {} WHERE", table);
                let condition = keys
                    .unwrap()
                    .iter()
                    .map(|(key, value)| format!(" \"{}\" = {}", key, value))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                sql_string.push_str(&condition);
                sql_string.push_str(";");
                return sql_string;
            }
        }
    }
    pub fn get_sql_columns(&self) -> Vec<String> {
        let mut columns = self
            .0
            .keys()
            .into_iter()
            .map(|key| format!("\"{}\"", key))
            .collect::<Vec<String>>();
        columns
    }
    pub fn create_sql_schema(&self) -> String {
        let mut sql = "(".to_string();
        for (key, value) in self.0.iter() {
            match value {
                Value::Number(_) => {
                    sql.push_str(&format!("\"{}\" INT, ", key));
                }
                Value::String(_) => {
                    sql.push_str(&format!("\"{}\" TEXT, ", key));
                }
                Value::Bool(_) => {
                    sql.push_str(&format!("\"{}\" BOOLEAN, ", key));
                }
                _ => {}
            }
        }
        sql.pop();
        sql.pop();
        sql.push_str(" );");
        sql
    }
    pub fn from_raw_pointer(raw: usize) -> Self {
        // Convert usize back to raw pointer
        let pointer = raw as *const DBRecord;

        // Use unsafe block for dereferencing and cloning
        unsafe {
            // Check for null pointer
            if pointer.is_null() {
                panic!("Attempted to dereference a null pointer");
            }

            // Dereference the pointer and clone the value
            (*pointer).clone()
        }
    }

    pub fn merge(&mut self, mut other: DBRecord) -> DBRecord {
        self.0.append(&mut other.0);
        return self.clone();
    }
    pub fn prefix_keys(&self, prefix: String) -> DBRecord {
        let mut record = BTreeMap::new();
        for (key, value) in self.0.iter() {
            record.insert(format!("{}.{}", prefix, key), value.clone());
        }
        DBRecord(record)
    }

    pub fn pick(&self, keys: Vec<String>) -> DBRecord {
        let mut record = BTreeMap::new();
        for key in keys {
            match self.0.get(&key) {
                Some(value) => {
                    record.insert(key, value.clone());
                }
                None => {}
            }
        }
        DBRecord(record)
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
