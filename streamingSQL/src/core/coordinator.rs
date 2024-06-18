use crate::core::parser::parse_query;

use crate::pg_client::pg_client::PGClient;

use super::planer::QueryPlaner;

pub struct Coordinator {
    pg_client: PGClient,
}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {
            pg_client: PGClient::new("localhost", "postgres").ok().unwrap(),
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
        Ok(())
    }
}
