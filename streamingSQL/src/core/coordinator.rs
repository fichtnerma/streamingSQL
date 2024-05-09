use std::{collections::HashMap, rc::Rc};

use crate::pg_client::pg_client::PGClient;

use super::worker::Worker;

pub struct Coordinator {
    view_worker_map: HashMap<String, Worker>,
    pg_client: Rc<PGClient>,
}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {
            view_worker_map: HashMap::new(),
            pg_client: Rc::new(PGClient::new("localhost", "postgres").ok().unwrap()),
        }
    }

    pub async fn process_view_query(
        &mut self,
        query: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.view_worker_map.contains_key(query) {
            let query_str = query.to_string();
            let worker = Worker::new(Rc::clone(&self.pg_client));
            worker.run().await?;
            self.view_worker_map.insert(query.to_string(), worker);
        }
        Ok(())
    }
}
