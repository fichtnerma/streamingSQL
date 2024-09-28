use std::collections::HashMap;

use tokio::task;
use tracing::debug;

use crate::core::parser::parse_query;

use crate::pg_client::data::WalEvent;
use crate::pg_client::stream::start_streaming_changes;

use super::planer::QueryPlaner;

pub struct Coordinator {
    channels: HashMap<String, tokio::sync::broadcast::Sender<Vec<WalEvent>>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {
            channels: HashMap::new(),
        }
    }

    pub async fn process_view_query(
        &mut self,
        query: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query_str = query.to_string();
        let mut query_info = parse_query(&query_str).unwrap();
        let planer = QueryPlaner::new();
        for table in &query_info.tables {
            let table_name = table.to_string();
            debug!("Listening to Table: {}", &table_name);
            let (tx, _) = tokio::sync::broadcast::channel::<Vec<WalEvent>>(100);
            let tx_clone = tx.clone();
            task::spawn(async { start_streaming_changes(tx_clone, table_name).await });
            self.channels.insert(table.to_string(), tx);
        }
        planer
            .build_dataflow(&mut query_info, self.channels.clone())
            .await;
        Ok(())
    }
}
