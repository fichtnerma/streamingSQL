use std::{env, sync::Arc};

use tokio::sync::broadcast::Sender;
use tracing::{debug, info};

use crate::pg_client::data::WalEvent;

use super::{publication, replication};

pub async fn start_streaming_changes(
    tx: Sender<Vec<WalEvent>>,
    table_name: String,
) -> Result<(), tokio_postgres::Error> {
    let ev = |name| env::var(name).unwrap();
    let db_config = format!(
        "user={} password={} host={} port={} dbname={}",
        ev("DB_USER"),
        ev("DB_PASSWORD"),
        ev("DB_ADDR"),
        ev("DB_PORT"),
        "postgres"
    );
    let repl_client = Arc::new(
        replication::DBClient::new(&format!("{} replication=database", db_config))
            .await
            .unwrap()
            .client,
    );

    let storage_client = Arc::new(replication::DBClient::new(&db_config).await.unwrap().client);

    // Hardcoded for now
    let schema_name = "public".to_string();

    let publication =
        publication::Publication::new(Arc::clone(&storage_client), &schema_name, &table_name);

    // Check if publication exists or create it if it doesn't
    if publication.check_exists().await? {
        debug!("Publication already exists");
    } else {
        debug!("Creating publication ");
        publication.create().await?;
    }

    let slot_name = format!("slot_{}", table_name.to_lowercase().trim_matches('"'));
    let mut slot = replication::Slot::new(Arc::clone(&repl_client), &slot_name);
    slot.get_confirmed_lsn().await?;
    // Create new replication slot if LSN is None
    if slot.lsn.is_none() {
        debug!("Creating replication slot {:?}", slot_name);
        slot.create().await?;
    }
    let mut replicator =
        replication::Replicator::new(Arc::clone(&repl_client), slot, publication, tx);
    replicator.start_replication().await;

    Ok(())
}
