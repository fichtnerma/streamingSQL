use std::collections::HashMap;

use tokio::task;
use tracing::{debug, info};

use crate::core::parser::parse_query;

use crate::core::types::source::Source;
use crate::pg_client::data::WalEvent;
use crate::pg_client::schema::get_keys_for_table;
use crate::pg_client::stream::start_streaming_changes;

use super::planer::QueryPlaner;

pub struct Coordinator {}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {}
    }

    pub async fn process_view_query(
        &mut self,
        query: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query_str = query.to_string();
        let query_info = parse_query(&query_str).unwrap();

        let mut source = Source::new();
        let mut table_identities = HashMap::new();
        for table in &query_info.tables {
            let table_name = table.to_string();
            let (tx, rx) = tokio::sync::broadcast::channel::<Vec<WalEvent>>(100000);
            task::spawn(async { start_streaming_changes(tx, table_name).await });
            source.insert(table.to_string(), rx);
            table_identities.insert(
                table.to_string(),
                get_keys_for_table(table.to_string()).await.unwrap(),
            );
        }

        let planer = QueryPlaner::new(table_identities);
        planer.build_dataflow(query_info, source).await;
        Ok(())
    }
}
