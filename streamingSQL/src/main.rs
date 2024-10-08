use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::{self};

mod core;
mod pg_client;
use core::coordinator::Coordinator;

// #[get("/view")]
// async fn create_view() -> impl Responder {
//     coordinator.process_view_query(query).await.unwrap();
//     HttpResponse::Ok().body("Created view!")
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
    let query = r#"SELECT "User".id, "User".name, "Order".total FROM "User" JOIN "Order" ON "User".id = "Order"."buyerId" WHERE "Order".total > 600"#;
    coordinator.process_view_query(query).await.unwrap();
    Ok(())
}

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
