use std::{collections::HashMap, env};

use tokio_postgres::{Error, NoTls};
use tracing::warn;

#[derive(Debug, Clone)]

pub enum KeyType {
    PrimaryKey,
    ForeignKey {
        foreign_table: String,
        foreign_column: String,
    },
}

#[derive(Debug, Clone)]
pub struct Key {
    pub column_name: String,
    pub key_type: KeyType,
}

// TODO: cache the keys
pub async fn get_keys_for_table(table_name: String) -> Result<Vec<Key>, Error> {
    let ev = |name| env::var(name).unwrap();

    let db_config = format!(
        "user={} password={} host={} port={} dbname={}",
        ev("DB_USER"),
        ev("DB_PASSWORD"),
        ev("DB_ADDR"),
        ev("DB_PORT"),
        "postgres"
    );

    // connect to the database
    let (client, connection) = tokio_postgres::connect(&db_config, NoTls).await.unwrap();

    // Spawn a task to manage the connection (this will run in the background)
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let query = format!(
        "SELECT 
            keys.column_name, 
            'PRIMARY KEY' AS key_type,
            NULL AS foreign_table,
            NULL AS foreign_column
        FROM 
            information_schema.table_constraints constraints
        JOIN 
            information_schema.key_column_usage keys 
            ON constraints.constraint_name = keys.constraint_name
        WHERE 
            constraints.constraint_type = 'PRIMARY KEY'
            AND keys.table_name = \'{table_name}\'

        UNION ALL

        SELECT 
            keys.column_name, 
            'FOREIGN KEY' AS key_type,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM 
            information_schema.table_constraints constraints
        JOIN 
            information_schema.key_column_usage keys 
            ON constraints.constraint_name = keys.constraint_name
        JOIN 
            information_schema.constraint_column_usage ccu 
            ON ccu.constraint_name = constraints.constraint_name
        WHERE 
            constraints.constraint_type = 'FOREIGN KEY'
            AND keys.table_name = \'{table_name}\'",
    );
    let mut keys: Vec<Key> = Vec::new();
    // Execute the query
    let rows = client.query(&query, &[]).await?;

    // Print out each column's schema information
    for row in rows {
        let column_name: &str = row.get(0);
        let key_type: &str = row.get(1);
        let foreign_table: Option<&str> = row.get(2);
        let foreign_column: Option<&str> = row.get(3);
        keys.push(Key {
            column_name: column_name.to_string(),
            key_type: match key_type {
                "PRIMARY KEY" => KeyType::PrimaryKey,
                "FOREIGN KEY" => KeyType::ForeignKey {
                    foreign_table: "".to_string(),
                    foreign_column: "".to_string(),
                },
                _ => panic!("Unknown key type"),
            },
        });
    }

    Ok(keys)
}
