pub struct QueryPlaner {}

impl QueryPlaner {
    pub fn plan(&self, plan: Plan) -> Result<(), Box<dyn std::error::Error>> {
        match plan {
            Plan::Insert => plan_insert(),
            Plan::Update => plan_update(),
            Plan::Delete => plan_delete(),
        }
    }

    fn plan_insert() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = Client::connect("host=localhost user=postgres", NoTls)?;

        let name = "Ferris";
        let data = None::<&[u8]>;
        client.execute(
            "INSERT INTO person (name, data) VALUES ($1, $2)",
            &[&name, &data],
        )?;
        Ok(())
    }

    fn plan_update() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = Client::connect("host=localhost user=postgres", NoTls)?;

        let publicationName = "myreplication";
        let tableName = "person";

        client.batch_execute(format!(
            "ALTER PUBLICATION {publicationName}! ADD TABLE {tableName}!"
        ))?;
        Ok(())
    }

    fn plan_delete() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = Client::connect("host=localhost user=postgres", NoTls)?;

        let publicationName = "myreplication";
        let slotName = "repl_slot";

        client.batch_execute(
            format!("DROP PUBLICATION {publicationName}!"),
            format!("SELECT pg_drop_replication_slot('{slotName}!')"),
        )?;
        Ok(())
    }
}
