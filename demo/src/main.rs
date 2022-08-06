#[macro_use]
extern crate derive_getters;

use std::env;


use actix_web::{App, HttpServer, web};
use anyhow::anyhow;
use opentelemetry::api::Provider;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use async_raft::NodeId;
//use futures::stream::FuturesUnordered;


use crate::apis::{add_learner, add_member, append_entries, bootstrap, hello, install_snapshot, metrics, pong, read, vote, write};
use crate::common::{DemoAppData, init_tracer, PEERS};
use crate::mem_raft::InMemRaft;

pub mod common;
mod mem_raft;
mod apis;

#[allow(dead_code)]
fn init_tracer_2() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = opentelemetry_jaeger::Exporter::builder()
        .with_agent_endpoint("127.0.0.1:6831".parse().unwrap())
        .with_process(opentelemetry_jaeger::Process {
            service_name: "Test-run".to_string(),
            tags: Vec::new(),
        })
        .init()?;
    let provider = opentelemetry::sdk::Provider::builder()
        .with_simple_exporter(exporter)
        .with_config(opentelemetry::sdk::Config {
            default_sampler: Box::new(opentelemetry::sdk::Sampler::AlwaysOn),
            ..Default::default()
        })
        .build();
    let tracer = provider.get_tracer("tracing");

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()?;

    Ok(())
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    /*init_tracer_2().expect("Tracer setup failed");
    let root = tracing::span!(tracing::Level::TRACE, "lifecycle");
    let _enter = root.enter();*/

    init_tracer();


    let node_id = parse_node_id();
    info!("starting raft server with id {node_id}");

    let app_data = web::Data::new(DemoAppData::new(node_id as NodeId));

    HttpServer::new(move || App::new()
        .configure(config)
        .app_data(app_data.clone())
    )
        .bind(PEERS[node_id as usize])?
        .workers(2)
        .run()
        .await
        .map_err(|e| anyhow!(e))
}

fn parse_node_id() -> u8 {
    env::var("NODE_ID")
        .map(|id| id.parse::<u8>().unwrap())
        .expect("invalid node id ")
}


fn config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(hello)
        .service(pong)

        .service(append_entries)
        .service(install_snapshot)
        .service(vote)

        .service(bootstrap)
        .service(metrics)
        .service(add_learner)
        .service(add_member)

        .service(write)
        .service(read);
}


