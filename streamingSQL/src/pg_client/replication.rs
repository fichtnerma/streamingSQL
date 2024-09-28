use futures::{
    future::{self},
    ready, Sink, StreamExt,
};
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{debug, info, warn};

use bytes::Bytes;

use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{
    types::{self, PgLsn},
    CopyBothDuplex, NoTls, SimpleQueryMessage,
};

use crate::pg_client::data::WalEvent;

use super::publication::Publication;

const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;

pub struct Slot {
    client: Arc<tokio_postgres::Client>,
    name: String,
    pub lsn: Option<PgLsn>,
}

impl Slot {
    pub fn new(client: Arc<tokio_postgres::Client>, slot_name: &String) -> Self {
        Self {
            client: client,
            name: slot_name.clone(),
            lsn: None,
        }
    }

    pub async fn get_confirmed_lsn(&mut self) -> Result<(), tokio_postgres::Error> {
        let query = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.name
        );
        let result = self.client.simple_query(&query).await?;
        let rows = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(slot) = rows.first() {
            let lsn = slot.get("confirmed_flush_lsn").unwrap().to_string();
            self.lsn = Some(lsn.parse::<PgLsn>().unwrap());
        }

        Ok(())
    }

    pub async fn create(&mut self) -> Result<(), tokio_postgres::Error> {
        let slot_query = format!(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"wal2json\"",
            self.name
        );
        let result = self.client.simple_query(&slot_query).await?;

        let lsn = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
            .first()
            .unwrap()
            .get("consistent_point")
            .unwrap()
            .to_owned();
        debug!("Created replication slot: {:?}", lsn);
        self.lsn = Some(lsn.parse::<PgLsn>().unwrap());
        Ok(())
    }
}

pub struct DBClient {
    pub client: tokio_postgres::Client,
}

impl DBClient {
    pub async fn new(db_config: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(db_config, NoTls).await?;
        tokio::spawn(async move { connection.await });
        Ok(Self { client })
    }
}

pub struct Replicator {
    commit_lsn: types::PgLsn,
    slot_name: String,
    schema_name: String,
    table_name: String,
    client: Arc<tokio_postgres::Client>,
    records: Vec<Value>,
    stream: Option<Pin<Box<CopyBothDuplex<Bytes>>>>,
    sender: tokio::sync::broadcast::Sender<Vec<WalEvent>>,
}

fn prepare_ssu(write_lsn: PgLsn) -> Bytes {
    let write_lsn_bytes = u64::from(write_lsn).to_be_bytes();
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .unwrap();

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii

    // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    Bytes::from(data_to_send)
}

impl Replicator {
    pub fn new(
        client: Arc<tokio_postgres::Client>,
        slot: Slot,
        publication: Publication,
        sender: tokio::sync::broadcast::Sender<Vec<WalEvent>>,
    ) -> Self {
        Self {
            schema_name: publication.schema_name.clone(),
            table_name: publication.table_name.clone(),
            // lsn must be assigned at this point else we panic
            commit_lsn: slot.lsn.unwrap().clone(),
            slot_name: slot.name.clone(),
            client,
            records: vec![],
            stream: None,
            sender,
        }
    }

    async fn commit(&mut self) {
        let buf = prepare_ssu(self.commit_lsn);
        self.send_ssu(buf).await;

        self.records.clear();
        debug!("Clearing records: {:?}", self.records);
    }

    async fn send_ssu(&mut self, buf: Bytes) {
        debug!("Trying to send SSU");
        let mut next_step = 1;
        future::poll_fn(|cx| loop {
            match next_step {
                1 => {
                    ready!(self.stream.as_mut().unwrap().as_mut().poll_ready(cx)).unwrap();
                }
                2 => {
                    self.stream
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .start_send(buf.clone())
                        .unwrap();
                }
                3 => {
                    ready!(self.stream.as_mut().unwrap().as_mut().poll_flush(cx)).unwrap();
                }
                4 => return Poll::Ready(()),
                _ => panic!(),
            }
            next_step += 1;
        })
        .await;
        debug!("Sent SSU");
    }

    fn replicate(&self) -> Result<(), tokio_postgres::Error> {
        debug!("Replicating...");
        debug!("{:?}", self.records);
        if self.records.len() >= 1 {
            let wal_events = self
                .records
                .iter()
                .map(|e| {
                    return WalEvent::from_wal_json(e.clone());
                })
                .collect::<Vec<_>>();
            self.sender.send(wal_events).unwrap();
        }
        Ok(())
    }

    async fn process_record(&mut self, record: Value) {
        match record["action"].as_str().unwrap() {
            // Begin of transaction
            "B" => {
                let lsn_str = record["nextlsn"].as_str().unwrap();
                self.commit_lsn = lsn_str.parse::<PgLsn>().unwrap();
            }
            // Commit of transaction
            "C" => {
                // (todo): handle error
                self.replicate().unwrap_or_else(|_| {
                    warn!("Ignoring replication error");
                });
                self.commit().await;
            }
            // Insert
            "I" => {
                debug!("Insert===");
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
                self.records.push(record);
            }
            // Update
            "U" => {
                debug!("Update===");
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
                self.records.push(record);
            }
            // Delete
            "D" => {
                debug!("Delete===");
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
                self.records.push(record);
            }
            _ => {
                debug!("unknown message");
            }
        }
    }

    async fn process_txn(&mut self, event: &[u8]) {
        match event[0] {
            b'w' => {
                // first 24 bytes are metadata
                let json: Value = serde_json::from_slice(&event[25..]).unwrap();
                self.process_record(json).await;
            }
            b'k' => {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                debug!(
                    "Got keepalive message @timeoutImminent:{}, @LSN:{:x?}",
                    timeout_imminent, self.commit_lsn,
                );
                if timeout_imminent {
                    let buf = prepare_ssu(self.commit_lsn);
                    self.send_ssu(buf).await;
                }
            }
            _ => (),
        }
    }

    pub async fn start_replication(&mut self) {
        let full_table_name = format!("{}.{}", self.schema_name, self.table_name);
        let options = vec![
            ("pretty-print", "false"),
            ("include-transaction", "true"),
            ("include-lsn", "true"),
            ("include-timestamp", "true"),
            ("include-pk", "true"),
            ("format-version", "2"),
            ("include-xids", "true"),
            ("add-tables", &full_table_name),
        ];
        let start_lsn = self.commit_lsn.to_string();
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} ({})",
            self.slot_name,
            start_lsn,
            // specify table for replication
            options
                .iter()
                .map(|(k, v)| format!("\"{}\" '{}'", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let duplex_stream = self
            .client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();

        // Pin the stream
        self.stream = Some(Box::pin(duplex_stream));

        // listen
        loop {
            match self.stream.as_mut().unwrap().next().await {
                Some(Ok(event)) => {
                    // (todo:) should return error?
                    self.process_txn(&event).await;
                }
                Some(Err(e)) => {
                    debug!("Error reading from stream:{}", e);
                    continue;
                }
                None => {
                    debug!("Stream closed");
                    break;
                }
            }
        }
    }
}
