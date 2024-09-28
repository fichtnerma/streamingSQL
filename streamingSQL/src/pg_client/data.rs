use std::collections::BTreeMap;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct WalEvent {
    pub timestamp: String,
    pub xid: i64,
    pub pkey: PKey,
    pub data: WalData,
}

#[derive(Debug, Clone)]
pub struct PKey {
    pub col: String,
    pub val: Value,
}

impl WalEvent {
    pub fn from_wal_json(value: Value) -> Self {
        WalEvent {
            timestamp: value["timestamp"].as_str().unwrap().to_string(),
            xid: value["xid"].as_i64().unwrap(),
            pkey: retrieve_pkey(value.clone()),
            data: WalData::from_wal_json(value.clone()),
        }
    }
}
fn retrieve_pkey(value: Value) -> PKey {
    if value["identity"].is_null() {
        let key_col = value["pk"].as_array().unwrap()[0].as_object().unwrap();
        let key_val = value["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|col| col["name"] == key_col["name"].as_str().unwrap())
            .unwrap()
            .as_object()
            .unwrap();
        return PKey {
            col: key_col["name"].as_str().unwrap().to_string(),
            val: key_val["value"].clone(),
        };
    } else {
        match value["identity"].as_array() {
            Some(obj) => {
                let identity = obj[0].as_object().unwrap();
                return PKey {
                    col: identity["name"].as_str().unwrap().to_string(),
                    val: identity["value"].clone(),
                };
            }
            None => panic!("No primary key found"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WalData {
    Insert(Insert),
    Update(Update),
    Delete,
}

impl WalData {
    pub fn from_wal_json(value: Value) -> Self {
        match value["action"].as_str().unwrap() {
            "I" => WalData::Insert(Insert::from_wal_json(value)),
            "U" => WalData::Update(Update::from_wal_json(value)),
            "D" => WalData::Delete,
            _ => panic!("Invalid WAL data kind"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Insert(pub BTreeMap<String, Value>);

impl Insert {
    pub fn from_wal_json(value: Value) -> Self {
        let mut values = BTreeMap::new();
        for col in value["columns"].as_array().unwrap() {
            let key = col["name"].as_str().unwrap().to_string();
            let value = col["value"].clone();
            values.insert(key, value);
        }
        Insert(values)
    }
}

#[derive(Debug, Clone)]
pub struct Update(pub BTreeMap<String, Value>);

impl Update {
    pub fn from_wal_json(value: Value) -> Self {
        let mut values = BTreeMap::new();
        for col in value["columns"].as_array().unwrap() {
            let key = col["name"].as_str().unwrap().to_string();
            let value = col["value"].clone();
            values.insert(key, value);
        }
        Update(values)
    }
}
