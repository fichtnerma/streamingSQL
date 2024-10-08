use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::FutureExt;
use tokio_postgres::{Client, GenericClient};
use tracing::{info, warn};

use crate::pg_client::replication;

#[derive(Debug, Clone)]
pub struct Sink {
    pub client: Arc<Client>,
    pub table: String,
    columns: Vec<String>,
    data: Vec<String>,
    table_created: bool,
}

impl Sink {
    pub async fn new(table: String) -> Self {
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
            replication::DBClient::new(&format!("{}", db_config))
                .await
                .unwrap()
                .client,
        );
        Sink {
            client: repl_client,
            table,
            columns: vec![],
            data: vec![],
            table_created: false,
        }
    }
    pub fn set_schema(&mut self, schema: String) {
        let run_time = tokio::runtime::Runtime::new().unwrap();
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} ", self.table);

        let schema = sql + &schema;

        match run_time.block_on(self.client.execute(schema.as_str(), &[])) {
            Ok(_) => {
                self.table_created = true;
            }
            Err(e) => {
                warn!("Error creating table: {}", e);
            }
        }
    }
    pub fn set_columns(&mut self, columns: Vec<String>) {
        self.columns = columns;
    }

    pub fn insert(&mut self, data: &mut Vec<String>) {
        self.data.append(data);
    }

    fn save(&mut self) {
        let run_time = tokio::runtime::Runtime::new().unwrap();

        let mut sql = "".to_string();
        let values = self.data.drain(..).collect::<Vec<String>>().join(" ");
        sql.push_str(&values);
        info!("Executed query: {}", sql);
        run_time
            .block_on(self.client.batch_execute(sql.as_str()))
            .unwrap();
    }

    pub fn execute_transaction(&mut self) {
        let run_time = tokio::runtime::Runtime::new().unwrap();
        run_time
            .block_on(self.client.execute("BEGIN TRANSACTION", &[]))
            .unwrap();
        self.save();
        run_time
            .block_on(self.client.execute("COMMIT TRANSACTION", &[]))
            .unwrap();
    }
}
