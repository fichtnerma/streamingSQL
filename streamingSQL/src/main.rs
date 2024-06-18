use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use sqlparser::{dialect::PostgreSqlDialect, parser};
mod core;
mod pg_client;
use core::coordinator::Coordinator;
use std::io;

static coordinator: Coordinator = Coordinator::new();

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello, world!")
}

#[get("/view")]
async fn create_view() -> impl Responder {
    coordinator.process_view_query(query).await.unwrap();
    HttpResponse::Ok().body("Created view!")
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    HttpServer::new(|| App::new().service(hello))
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
