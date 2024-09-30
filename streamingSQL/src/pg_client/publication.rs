use std::sync::Arc;
use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, info, warn};

pub struct Publication {
    pub client: Arc<tokio_postgres::Client>,
    pub schema_name: String,
    pub table_name: String,
}

impl Publication {
    pub fn new(
        client: Arc<tokio_postgres::Client>,
        schema_name: &String,
        table_name: &String,
    ) -> Self {
        Self {
            client: client,
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
        }
    }

    #[inline]
    pub fn pub_name(&self) -> String {
        format!("pub_{}", self.table_name.to_lowercase().trim_matches('"'))
    }

    pub async fn check_exists(&self) -> Result<bool, tokio_postgres::Error> {
        let pub_name = self.pub_name();
        let query = format!(
            "SELECT schemaname, tablename
                FROM pg_publication p
                JOIN pg_publication_tables pt ON p.pubname = pt.pubname
                WHERE p.pubname = '{}'",
            pub_name
        );
        let result = self.client.simple_query(&query).await?;
        let rows = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(publication) = rows.first() {
            let schema_name = publication.get("schemaname").unwrap().to_string();
            let table_name = publication.get("tablename").unwrap().to_string();
            debug!(
                "Found publication {:?}/{:?}, ready to start replication",
                schema_name, table_name
            );
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    pub async fn create(&self) -> Result<u64, tokio_postgres::Error> {
        warn!("Publication for: {} {}", self.pub_name(), self.table_name);
        let query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            self.pub_name(),
            self.table_name
        );
        debug!("Creating publication: {:?}", query);
        let result = self.client.execute(&query, &[]).await.unwrap();
        info!("Created publication: {:?}", result);
        Ok(result)
    }
}
