use axum::extract::State;
use axum::{extract::Path, http::Method, Json, Router};
use chrono::Utc;
use deadpool_redis::Pool;
use log::debug;
use serde_json::{json, Value};
use server::catchup::catchup;
use server::check_timeout::check_timeout;
use server::scheduler::scheduler;
use server::statics::{_get_default_edges, _get_default_tasks, _get_options};
use server::{_get_all_task_results, get_redis_pool};
use server::{
    _get_all_tasks, _get_dags, _get_task, _get_task_result, _get_task_status, _trigger_run,
    redis_runner::RedisRunner,
};
use tower_http::services::ServeDir;
use std::path::PathBuf;
use std::str::from_utf8;
use thepipelinetool::prelude::*;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use axum::routing::get;
use timed::timed;

#[timed(duration(printer = "debug!"))]
async fn ping() -> &'static str {
    "pong"
}

#[timed(duration(printer = "debug!"))]
async fn get_runs(Path(dag_name): Path<String>, State(pool): State<Pool>) -> Json<Value> {
    json!(RedisRunner::get_runs(&dag_name, pool)
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
    State(pool): State<Pool>,
) -> Json<Value> {
    let mut res = json!({});

    for run in RedisRunner::get_runs(&dag_name, pool.clone()).await.iter() {
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
}

// async fn get_options(Path(dag_name): Path<String>) -> Json<Value> {
//     _get_options(&dag_name).into()
// }

async fn get_default_tasks(Path(dag_name): Path<String>) -> Json<Value> {
    serde_json::to_value(&_get_default_tasks(&dag_name))
        .unwrap()
        .into()
}

async fn get_default_task(Path((dag_name, task_id)): Path<(String, usize)>) -> Json<Value> {
    json!(&_get_default_tasks(&dag_name)
        .iter()
        .find(|t| t.id == task_id)
        .unwrap())
    .into()
}

async fn get_all_tasks(Path(run_id): Path<usize>, State(pool): State<Pool>) -> Json<Value> {
    json!(_get_all_tasks(run_id, pool)).into()
}

async fn get_task(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<Pool>,
) -> Json<Value> {
    json!(_get_task(run_id, task_id, pool)).into()
}

async fn get_all_task_results(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<Pool>,
) -> Json<Value> {
    json!(_get_all_task_results(run_id, task_id, pool).await).into()
}

async fn get_task_status(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<Pool>,
) -> String {
    from_utf8(&[_get_task_status(run_id, task_id, pool).as_u8()])
        .unwrap()
        .to_owned()
}

async fn get_task_result(
    Path((run_id, task_id)): Path<(usize, usize)>,
    State(pool): State<Pool>,
) -> Json<Value> {
    json!(_get_task_result(run_id, task_id, pool)).into()
}

async fn get_task_log(
    Path((run_id, task_id, attempt)): Path<(usize, usize, usize)>,
    State(pool): State<Pool>,
) -> String {
    RedisRunner::dummy(pool).get_log(run_id, task_id, attempt)
}

async fn get_dags() -> Json<Value> {
    let mut result: Vec<Value> = vec![];

    for dag_name in _get_dags() {
        let mut options = serde_json::to_value(_get_options(&dag_name)).unwrap();
        options["dag_name"] = dag_name.into();
        result.push(options);
    }

    json!(result).into()
}

async fn get_run_graph(Path(run_id): Path<usize>, State(pool): State<Pool>) -> Json<Value> {
    json!(RedisRunner::dummy(pool).get_graphite_graph(run_id)).into()
}

async fn get_default_graph(Path(dag_name): Path<String>) -> Json<Value> {
    let nodes = _get_default_tasks(&dag_name);
    let edges = _get_default_edges(&dag_name);
    let mut runner = InMemoryRunner::new(&nodes, &edges);
    runner.enqueue_run("in_memory", "", Utc::now());

    json!(runner.get_graphite_graph(0)).into()
}

async fn trigger(Path(dag_name): Path<String>, State(pool): State<Pool>) {
    tokio::spawn(async move {
        _trigger_run(&dag_name, Utc::now(), pool).await;
    });
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let pool = get_redis_pool();

    let now = Utc::now();

    // catchup(&now, pool.clone());
    // scheduler(&now, pool.clone());
    check_timeout(pool.clone());

    let app = Router::new()
        .nest_service("/", ServeDir::new(PathBuf::from("static")))
        .route("/ping", get(ping))
        .route("/dags", get(get_dags))
        .route("/runs/:dag_name", get(get_runs))
        .route("/runs_with_tasks/:dag_name", get(get_runs_with_tasks))
        .route("/trigger/:dag_name", get(trigger))
        .route("/task_status/:run_id/:task_id", get(get_task_status))
        .route("/task_result/:run_id/:task_id", get(get_task_result))
        .route("/log/:run_id/:task_id/:attempt", get(get_task_log))
        .route("/tasks/:run_id", get(get_all_tasks))
        .route("/task_results/:run_id/:task_id", get(get_all_task_results))
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
        .with_state(pool);

    axum::Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
