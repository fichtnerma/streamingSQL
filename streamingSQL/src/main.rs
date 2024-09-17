// use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use pg_client::{replication::{start_streaming_changes, Transaction}, subscriber::subscribe};
use tokio::task;
// use sqlparser::dialect::{PostgreSqlDialect};
// use sqlparser::parser::Parser;

// mod core;
mod pg_client;
// use core::coordinator::Coordinator;
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
    let _sql = "SELECT customers.customer_id, customers.customer_name, orders.order_id, orders.order_date FROM customers JOIN orders ON customers.customer_id = orders.customers_id WHERE orders.amount > 1000";
    // let mut coordinator = Coordinator::new();
    // coordinator.process_view_query(sql).await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (tx, _) = tokio::sync::broadcast::channel::<Transaction>(100);
    let tx_clone = tx.clone();
    let listener_handle = task::spawn(async move {
        start_streaming_changes(ready_tx, tx_clone).await
    });
    // block waiting for replication
    ready_rx.await.unwrap();

    let tx_clone = tx.clone();

    let (done_tx, done_rx) = tokio::sync::mpsc::channel::<()>(1);
    let sub_handle =
        task::spawn(async move { subscribe(tx_clone, done_rx).await });

    listener_handle.await.unwrap().unwrap();
    sub_handle.await.unwrap();
    done_tx.send(()).await.unwrap();
    Ok(())
}

// fn main () -> io::Result<()> {
//     let sql = "SELECT customers.customer_id, customers.customer_name, orders.order_id, orders.order_date FROM customers JOIN orders ON customers.customer_id = orders.customers_id WHERE orders.amount > 1000";
//     let dialect = PostgreSqlDialect {};
//     let ast = Parser::parse_sql(&dialect, sql).unwrap();
//     println!("AST: {:?}", ast);
//     Ok(())
// }