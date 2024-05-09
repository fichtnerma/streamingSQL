use std::rc::Rc;

use crate::pg_client::pg_client::PGClient;

pub struct Worker {
    pg_client: Rc<PGClient>,
}

impl Worker {
    pub fn new(pg_client: Rc<PGClient>) -> Self {
        Worker {
            pg_client: pg_client,
        }
    }

    pub async fn listen_to_changes(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let name = "Ferris";
        let data = None::<&[u8]>;
        self.pg_client.execute(
            "INSERT INTO person (name, data) VALUES ($1, $2)",
            &[&name, &data],
        )?;
        Ok(())
    }
}
