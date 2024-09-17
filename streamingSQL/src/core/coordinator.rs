use tokio::task;

use crate::core::parser::parse_query;

use crate::pg_client::replication::{start_streaming_changes, Transaction};
use crate::pg_client::subscriber::subscribe;

use super::planer::QueryPlaner;

pub struct Coordinator {
}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {
        }
    }

    pub async fn process_view_query(
        &mut self,
        query: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query_str = query.to_string();
        let query_info = parse_query(&query_str).unwrap();
        let planer = QueryPlaner::new();
        planer.plan(query_info);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let (tx, _rx) = tokio::sync::broadcast::channel::<Transaction>(100);
        let tx_clone = tx.clone();
        let streaming_handle =
            task::spawn(async { start_streaming_changes(ready_tx, tx_clone).await });
    
        // block waiting for replication
        ready_rx.await.unwrap();

        let tx_clone = tx.clone();

        let (done_tx, done_rx) = tokio::sync::mpsc::channel::<()>(1);

        let subscriber_handle = task::spawn(async move { subscribe(tx_clone, done_rx).await });
    
        streaming_handle.await.unwrap().unwrap();
        subscriber_handle.await.unwrap();
        Ok(())
    }
}
