#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};


use actix_web::{get, HttpResponse, post, put, Responder, ResponseError, web};
use actix_web::web::{Data, Json};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::{info, trace};

use async_raft::{NodeId, RaftMetrics};
use async_raft::raft::{AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, ClientWriteResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use persiststore::{ClientRequest, ClientResponse};

use crate::{DemoAppData, PEERS};

type JsonResult<T> = actix_web::Result<web::Json<T>>;

pub struct DemoError {
    inner: anyhow::Error,
}

impl Debug for DemoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Display for DemoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl ResponseError for DemoError {}


#[post("/append_entries")]
pub async fn append_entries(state: web::Data<DemoAppData>, request: web::Json<AppendEntriesRequest<ClientRequest>>) -> actix_web::Result<web::Json<AppendEntriesResponse>> {
    println!("append_entries with requests {request:?}");
    let request = request.into_inner();
   /* if request.entries.len() != 0 {
        println!("append_entries with request {request:?}");
    }*/
    let resp = state.raft
        .append_entries(request).await
        .map_err(|e| DemoError { inner: e.into() });

    println!("append_entries with response {resp:?}");

    Ok(web::Json(resp?))
}

#[post("/install_snapshot")]
pub async fn install_snapshot(state: web::Data<DemoAppData>, request: web::Json<InstallSnapshotRequest>) -> JsonResult<InstallSnapshotResponse> {
    let request = request.into_inner();
    println!("install_snapshot with request term  {}, last_included_index = {}, last_included_term = {} ", request.term, request.last_included_index, request.last_included_term);
    let resp = state.raft
        .install_snapshot(request).await
        .map_err(|e| DemoError { inner: e.into() });

    println!("install_snapshot with response {resp:?}");
    println!();

    Ok(web::Json(resp?))
}

#[post("/vote")]
pub async fn vote(state: web::Data<DemoAppData>, request: web::Json<VoteRequest>) -> actix_web::Result<web::Json<VoteResponse>> {
    println!("vote with request {request:?}");
    let request = request.into_inner();
    let resp = state.raft
        .vote(request).await
        .map_err(|e| DemoError { inner: e.into() })?;

    Ok(web::Json(resp))
}

#[post("/bootstrap")]
pub async fn bootstrap(state: web::Data<DemoAppData>) -> impl Responder {
    tracing::info!("--- initializing cluster -----");
    bootstrap_cluster(state).await;
    "true"
}

async fn bootstrap_cluster(state: Data<DemoAppData>) {
    let mut members = HashSet::new();
    members.insert(0);
    //members.insert(1);
    //  members.insert(2); // TODO: remove this
    state.raft.initialize(members).await.unwrap();
}

#[post("/add_learner/{id}")]
pub async fn add_learner(state: web::Data<DemoAppData>, id: web::Path<u64>) -> JsonResult<RaftMetrics> {
    let id = id.into_inner();
    state.raft.add_non_voter(id).await.map_err(|e| DemoError { inner: e.into() })?;
    let m = get_metrics(state);
    Ok(web::Json(m))
}
#[post("/add_member/{id}")]
pub async fn add_member(state: web::Data<DemoAppData>, id: web::Path<u64>) -> JsonResult<RaftMetrics> {
    let id = id.into_inner();
    let m = get_metrics(state.clone());
    let mut set = m.membership_config.members;
    set.insert(id);
    state.raft.change_membership(set).await.map_err(|e| DemoError { inner: e.into() })?;
    Ok(web::Json(get_metrics(state.clone())))
}

#[get("/metrics")]
pub async fn metrics(state: web::Data<DemoAppData>) -> impl Responder {
    let metrics = get_metrics(state);
    web::Json(metrics)
}

fn get_metrics(state: Data<DemoAppData>) -> RaftMetrics {
    let receiver = state.raft.metrics();
    let m = receiver.borrow();
    let m = RaftMetrics {
        id: m.id,
        state: m.state,
        current_term: m.current_term,
        last_log_index: m.last_log_index,
        last_applied: m.last_applied,
        current_leader: m.current_leader,
        membership_config: m.membership_config.clone(),
    };
    m
}


#[tracing::instrument(skip(data))]
#[get("/")]
pub async fn hello(data: web::Data<DemoAppData>) -> actix_web::Result<impl Responder> {
    info!("calling hello api /");
    let id = data.node_id();
    Ok(HttpResponse::Ok().body(format!("hello from node {id}!")))
}

#[get("/ping")]
pub async fn pong() -> impl Responder {
    HttpResponse::Ok().body("pong")
}

#[tracing::instrument(level = "trace", skip(state))]
#[put("/write")]
pub async fn write(state: web::Data<DemoAppData>, request: web::Json<ClientRequest>) -> JsonResult<ClientWriteResponse<ClientResponse>> {
    trace!("got request for write");
    let resp = state.raft
        .client_write(ClientWriteRequest::new(request.into_inner()))
        .await
        .map_err(|e| DemoError { inner: e.into() })?;
    Ok(Json(resp))
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct GetResponse {
    client: String,
    response: Option<(u64, Option<String>)>,
    status: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ForwardToLeaderMeta {
    node_id: NodeId,
    addr: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ReadRespStatus { Success, ForwardToLeader }

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadResponse {
    status: ReadRespStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<ClientRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forward: Option<ForwardToLeaderMeta>,

}


#[derive(Debug, Deserialize)]
pub struct ReadQueryParams {
    consistent: Option<bool>,
}

#[get("/read/{client_id}")]
pub async fn read(state: web::Data<DemoAppData>, client_id: web::Path<String>, query: web::Query<ReadQueryParams>) -> JsonResult<ReadResponse> {
    let query_params = query.into_inner();
    let is_consistent_read = query_params.consistent.unwrap_or_else(|| false);
    println!("got request for /get with consistent read {:?}", is_consistent_read);

    if is_consistent_read && !state.is_current_leader().await {
        let leader = state.raft.current_leader().await;
        return match leader {
            None => {
                let error = DemoError { inner: anyhow!("no leader found") };
                Err(error.into())
            },
            Some(id) => {
                let resp = ReadResponse {
                    status: ReadRespStatus::ForwardToLeader,
                    data: None,
                    forward: Some(ForwardToLeaderMeta {
                        node_id: id,
                        addr: PEERS[id as usize].to_string(),
                    })
                };
                Ok(Json(resp))
            }
        }

    }


    let client_id = client_id.into_inner();
    let state_machine = state.store.state_machine();
    let status = state_machine.get(client_id.as_bytes());
    let response = status.map(|v| serde_json::from_slice::<ClientRequest>(&v).unwrap());

    Ok(Json(ReadResponse { status: ReadRespStatus::Success, data: response, forward: None }))
}

