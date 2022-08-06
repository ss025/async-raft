use std::sync::{Arc, Mutex};

use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use async_raft::NodeId;
use memstore::MemStore;

use crate::{InMemRaft, mem_raft};

pub(crate) const PEERS: [&str; 3] = ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"];

#[derive(Getters)]
pub struct DemoAppData {
    _counter: Mutex<i32>,
    node_id: NodeId,
    pub raft: InMemRaft,
    pub store: Arc<MemStore>,
}

impl DemoAppData {
    pub fn new(id: NodeId) -> Self {
        let storage = Arc::new(MemStore::new(id));
        DemoAppData {
            _counter: Mutex::new(0),
            node_id: id,
            raft: mem_raft::new(id, storage.clone()),
            store: storage,
        }
    }

    pub async fn is_current_leader(&self) -> bool {
        self.raft
            .current_leader()
            .await
            .filter(|m| *m == self.node_id)
            .is_some()
    }
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