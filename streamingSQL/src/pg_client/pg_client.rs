use postgres::{Client, NoTls};

pub struct PGClient {
    client: Client,
}

impl PGClient {
    pub fn new(host: &str, user: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = Client::connect(format!("host={host} user={user}").as_str(), NoTls)?;
        Ok(PGClient { client })
    }

    fn create_publication(&self, publication_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .batch_execute(format!("CREATE PUBLICATION {publication_name}").as_str())?;
        Ok(())
    }
    fn subscribe_replication(
        &self,
        publication_name: &str,
        slot_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client.batch_execute(
            format!(
                "CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost user=postgres dbname=postgres' PUBLICATION {publication_name}!"
            ).as_str(),
        )?;
        self.client.batch_execute(
            format!("ALTER SUBSCRIPTION mysub SET (slot_name = '{slot_name}!')").as_str(),
        )?;
        Ok(())
    }

    pub async fn init_replication(
        &mut self,
        pub_name: &str,
        slot_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.create_publication(pub_name)?;
        self.subscribe_replication(pub_name, slot_name)?;
        self.client.batch_execute(
            format!("SELECT pg_create_logical_replication_slot('{slot_name}!', 'pgoutput')")
                .as_str(),
        )?;

        Ok(())
    }

    fn update_replication(&self) -> Result<(), Box<dyn std::error::Error>> {
        let publication_name = "myreplication";
        let table_name = "person";

        self.client.batch_execute(
            format!("ALTER PUBLICATION {publication_name}! ADD TABLE {table_name}!").as_str(),
        )?;
        Ok(())
    }

    fn drop_replication(&self) -> Result<(), Box<dyn std::error::Error>> {
        let publication_name = "myreplication";
        let slot_name = "repl_slot";

        self.client
            .batch_execute(format!("DROP PUBLICATION {publication_name}!").as_str())?;
        self.client
            .batch_execute(format!("SELECT pg_drop_replication_slot('{slot_name}!')").as_str())?;
        Ok(())
    }
}
