use pg_client::data::WalEvent;
use pg_client::stream::start_streaming_changes;
// use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use tokio::task;
use tracing::level_filters::LevelFilter;
use tracing::{debug, info};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::{self};
// use sqlparser::dialect::{PostgreSqlDialect};
// use sqlparser::parser::Parser;

mod core;
mod pg_client;
use core::coordinator::Coordinator;
use std::io;

// #[get("/")]
// async fn hello() -> impl Responder {
//     HttpResponse::Ok().body("Hello, world!")
// }

// #[get("/view")]
// async fn create_view() -> impl Responder {
//     coordinator.process_view_query(query).await.unwrap();
//     HttpResponse::Ok().body("Created view!")
// }

// #[get("/stream")]
// async fn listen_stream() -> impl Responder {
//     start_streaming_changes();
//     HttpResponse::Ok().body("Listening to Postgres WAL!")

// }

// #[actix_web::main]
// async fn main() -> io::Result<()> {
//     HttpServer::new(|| App::new().service(hello))
//         .bind("127.0.0.1:8080")?
//         .run()
//         .await
// }

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    dotenv::dotenv().ok();
    init_logger();
    let mut coordinator = Coordinator::new();
    let query = r#"SELECT "User".id, "User".name, "Order".total FROM "User" JOIN "Order" ON "User".id = "Order"."buyerId""#;
    coordinator.process_view_query(query).await.unwrap();
    Ok(())
}

// #[tokio::main]
// async fn main() -> Result<(), tokio_postgres::Error> {
//     dotenv::dotenv().ok();
//     init_logger();
//     // let _sql = "SELECT customers.customer_id, customers.customer_name, orders.order_id, orders.order_date FROM customers JOIN orders ON customers.customer_id = orders.customers_id WHERE orders.amount > 1000";
//     let table_name = "User".to_string();
//     debug!("Listening to Table: {}", &table_name);
//     let (tx, _rx) = tokio::sync::broadcast::channel::<Vec<WalEvent>>(100);
//     let tx_clone = tx.clone();
//     let (_done_tx, mut done_rx) = tokio::sync::broadcast::channel::<()>(1);
//     let streaming_handle =
//         task::spawn(async { start_streaming_changes(tx_clone, table_name).await });
//     let mut rx = tx.subscribe();
//     loop {
//         tokio::select! {
//             _ = done_rx.recv() => {
//                 break
//             }
//             Ok(events) = rx.recv() => {
//                 for event in events {
//                     info!("Received event: {:?}", event);
//                 }
//             }
//         }
//     }
//     streaming_handle.await.unwrap().unwrap();

//     Ok(())
// }

fn init_logger() {
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}

// fn main () -> io::Result<()> {
//     let sql = "SELECT customers.customer_id, customers.customer_name, orders.order_id, orders.order_date FROM customers JOIN orders ON customers.customer_id = orders.customers_id WHERE orders.amount > 1000";
//     let dialect = PostgreSqlDialect {};
//     let ast = Parser::parse_sql(&dialect, sql).unwrap();
//     println!("AST: {:?}", ast);
//     Ok(())
// }
