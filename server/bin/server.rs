use std::collections::HashSet;
use std::str::from_utf8;
// use std::str::from_utf8;

use axum::extract::State;
use axum::{extract::Path, http::Method, Json, Router};
use chrono::Utc;
use log::debug;

// use runner::{
//     local::{hash_dag, LocalRunner},
//     DefRunner, Runner,
// };
// use runner::{local::hash_dag, DefRunner, Runner};
use serde_json::{json, Value};
use server::{
    _get_all_tasks, _get_dags, _get_default_edges, _get_default_tasks, _get_options, _get_task,
    _get_task_result, _get_task_status, _trigger_run, catchup::catchup, db::Db,
    scheduler::scheduler,
};
use server::{_get_hash, get_client};
use sqlx::PgPool;
use thepipelinetool::prelude::*;
use tower_http::compression::CompressionLayer;
// use task::task::Task;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use axum::routing::get;
use timed::timed;

// #[macro_use] extern crate log;

#[timed(duration(printer = "debug!"))]
async fn ping() -> &'static str {
    "pong"
}

#[timed(duration(printer = "debug!"))]
async fn get_runs(Path(dag_name): Path<String>, State(pool): State<PgPool>) -> Json<Value> {
    json!(Db::get_runs(&dag_name, pool)
        .await
        .iter()
        .map(|r| json!({
            "run_id": r.run_id.to_string(),
            "date": r.date,
        }))
        .collect::<Vec<Value>>())
    .into()
}

// TODO return only statuses?
async fn get_runs_with_tasks(
    Path(dag_name): Path<String>,
    State(pool): State<PgPool>,
) -> Json<Value> {
    let mut res = json!({});

    for run in Db::get_runs(&dag_name, pool.clone()).await.iter() {
        let mut tasks = json!({});
        for task in _get_all_tasks(run.run_id, pool.clone()) {
            tasks[format!("{}_{}", task.function_name, task.id)] = json!(task);
        }
        res[run.run_id.to_string()] = json!({
            "date": run.date,
            "tasks": tasks,
        });
    }
    res.into()
    // json!(Db::get_runs(&dag_name)
    //     .await
    //     .iter()
    //     .map(|run_id| _get_run_tasks(&dag_name, *run_id))
    //     .collect::())
    // .into()
}

// async fn get_options(Path(dag_name): Path<String>) -> Json<Value> {
//     _get_options(&dag_name).into()
// }

async fn get_default_tasks(Path(dag_name): Path<String>) -> Json<Value> {
    let v: Value = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();

    v.into()
}

async fn get_default_task(Path((dag_name, task_id)): Path<(String, usize)>) -> Json<Value> {
    let v: Vec<Task> = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();

    Json(json!(v.iter().filter(|t| t.id == task_id).next().unwrap()))
}

async fn get_all_tasks(Path(run_id): Path<usize>, State(pool): State<PgPool>) -> Json<Value> {
    json!(_get_all_tasks(run_id, pool)).into()
}

async fn get_task(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<PgPool>,
) -> Json<Value> {
    json!(_get_task(run_id, task_id, pool)).into()
}

async fn get_task_status(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<PgPool>,
) -> String {
    from_utf8(&[_get_task_status(run_id, task_id, pool).as_u8()])
        .unwrap()
        .to_owned()
}

async fn get_task_result(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<PgPool>,
) -> Json<Value> {
    json!(_get_task_result(run_id, task_id, pool)).into()
}

async fn get_task_log(
    Path((run_id, task_id, attempt)): Path<(usize, usize, usize)>,
    State(pool): State<PgPool>,
) -> String {
    let mut runner = Db::new(&"", &[], &HashSet::new(), pool);
    runner.get_log(&run_id, &task_id, attempt)
    // json!(_get_task_result(run_id, task_id, pool)).into()
}

async fn get_dags() -> Json<Value> {
    let mut res: Vec<Value> = vec![];

    for dag_name in _get_dags() {
        let mut o: Value = serde_json::from_str(&_get_options(&dag_name).await).unwrap();
        o["dag_name"] = dag_name.into();

        res.push(o);
    }

    // dbg!(&res);

    json!(res).into()
}

async fn get_run_graph(Path(run_id): Path<usize>, State(pool): State<PgPool>) -> Json<Value> {
    let mut runner = Db::new(&"", &[], &HashSet::new(), pool);
    let graph = runner.get_graphite_graph(&run_id);
    json!(graph).into()
}

async fn get_default_graph(Path(dag_name): Path<String>) -> Json<Value> {
    let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_str(&_get_default_edges(&dag_name).await).unwrap();
    let mut runner = LocalRunner::new("", &nodes, &edges);
    runner.enqueue_run("local", "", Utc::now());
    let graph = runner.get_graphite_graph(&0);

    json!(graph).into()
}

async fn trigger(Path(dag_name): Path<String>, State(pool): State<PgPool>) {
    tokio::spawn(async move {
        _trigger_run(&dag_name, Utc::now(), pool).await;
    });
}

async fn _trigger_local_run(Path(dag_name): Path<String>, State(pool): State<PgPool>) {
    let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_str(&_get_default_edges(&dag_name).await).unwrap();
    let mut runner = Db::new(&dag_name, &nodes, &edges, pool);
    let dag_run_id = runner.enqueue_run(&dag_name, &_get_hash(&dag_name).await, Utc::now());
    runner.run(&dag_run_id, 1);
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    // initialize the logger in the environment? not really sure.
    env_logger::init();

    let pool: sqlx::Pool<sqlx::Postgres> = get_client().await;

    Db::init_tables(pool.clone()).await;

    let now = Utc::now();

    // catchup(&now, pool.clone());
    // scheduler(&now, pool.clone());

    let app = Router::new()
        .route("/ping", get(ping))
        //
        .route("/dags", get(get_dags))
        //
        // .route("/options/:dag_name", get(get_options))
        .route("/runs/:dag_name", get(get_runs))
        .route("/runs_with_tasks/:dag_name", get(get_runs_with_tasks))
        .route("/trigger/:dag_name", get(trigger))
        //
        .route("/task_status/:run_id/:task_id", get(get_task_status))
        .route("/task_result/:run_id/:task_id", get(get_task_result))
        .route("/log/:run_id/:task_id/:attempt", get(get_task_log))
        .route("/tasks/:run_id", get(get_all_tasks))
        .route("/task/:run_id/:task_id", get(get_task))
        .route("/default_tasks/:dag_name", get(get_default_tasks))
        .route("/default_task/:dag_name/:task_id", get(get_default_task))
        .route("/graph/:run_id", get(get_run_graph))
        .route("/default_graph/:dag_name", get(get_default_graph))
        .layer(
            CorsLayer::new()
                .allow_methods([Method::GET, Method::POST])
                .allow_origin(Any),
        )
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        // .layer()
        .with_state(pool);
    // .with_state(redis);

    axum::Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
