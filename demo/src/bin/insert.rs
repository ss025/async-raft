
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, Level};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use serde:: {Serialize, Deserialize};

#[tokio::main]
async fn main() -> anyhow::Result<()>{
    init_tracer();
    let client = reqwest::Client::new();

    let metrics = client.get("http://127.0.0.1:7000/metrics").send().await?.text().await?;
    info!("bootstrapped server with {metrics}");

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct ClientRequest {
        pub client: String,
        pub serial: u64,
        pub status: String,
    }

    impl  ClientRequest {
        fn new(id: i32) -> Self{
            ClientRequest {
                client: format!("abc_{id}"),
                serial: id as u64,
                status: "active".to_string()
            }
        }
    }


    // insert
   for i in 1..=3 {
   // for i in 4..=5 {
        let request = ClientRequest::new(i);
        let c = client.put("http://127.0.0.1:7000/write").json(&request).send().await?;
        println!("requested {i} for resp {:?}",c);
    }

    sleep(Duration::from_secs(100 * 1000)).await;
    Ok(())
}



pub fn init_tracer() {
    let filter = EnvFilter::from_default_env()
        .add_directive(LevelFilter::TRACE.into())
        //.add_directive("my_crate::my_mod=debug".parse()?);
        .add_directive("actix_http::h1=error".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap());

    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        .with_env_filter(filter)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}