use std::collections::HashSet;

use axum::{extract::Path, http::Method, Json, Router};
use chrono::Utc;
use runner::{local::LocalRunner, DefRunner, Runner};
// use runner::{local::hash_dag, DefRunner, Runner};
use serde_json::{json, Value};
use server::{
    _get_dags, _get_edges, _get_options, _get_tasks, _trigger_run, catchup::catchup, db::Db,
    scheduler::scheduler,
};
use task::task::Task;
// use task::task::Task;
use tower_http::cors::{Any, CorsLayer};

use axum::routing::get;

async fn ping() -> &'static str {
    "pong"
}

async fn get_runs(Path(dag_name): Path<String>) -> Json<Value> {
    json!(Db::get_runs(&dag_name).await).into()
}

async fn get_options(Path(dag_name): Path<String>) -> Json<Value> {
    _get_options(&dag_name).into()
}

async fn get_default_tasks(Path(dag_name): Path<String>) -> Json<Value> {
    _get_tasks(&dag_name).into()
}

async fn get_run_tasks(Path((dag_name, run_id)): Path<(String, usize)>) -> Json<Value> {
    let runner = Db::new(&dag_name, &[], &HashSet::new());
    json!(runner.get_all_tasks(&run_id)).into()
}

async fn get_dags() -> Json<Value> {
    json!(_get_dags()).into()
}

async fn get_run_graph(Path((dag_name, run_id)): Path<(String, usize)>) -> Json<Value> {
    let runner = Db::new(&dag_name, &[], &HashSet::new());
    let graph = runner.get_graphite_graph(&run_id);
    json!(graph).into()
}

async fn get_default_graph(Path(dag_name): Path<String>) -> Json<Value> {
    let nodes: Vec<Task> = serde_json::from_value(_get_tasks(&dag_name)).unwrap();
    let edges: HashSet<(usize, usize)> = serde_json::from_value(_get_edges(&dag_name)).unwrap();
    let mut runner = LocalRunner::new("", &nodes, &edges);
    runner.enqueue_run("local", "", Utc::now().into());
    let graph = runner.get_graphite_graph(&0);

    json!(graph).into()
}

async fn trigger(Path(dag_name): Path<String>) {
    _trigger_run(&dag_name, Utc::now().into());
}

#[tokio::main]
async fn main() {
    Db::init_tables().await;

    let now = Utc::now();

    catchup(&now);
    scheduler(&now);

    let app = Router::new()
        .route("/ping", get(ping))
        //
        .route("/dags", get(get_dags))
        //
        .route("/options/:dag_name", get(get_options))
        .route("/runs/:dag_name", get(get_runs))
        .route("/trigger/:dag_name", get(trigger))
        //
        .route("/tasks/:dag_name/:run_id", get(get_run_tasks))
        .route("/default_tasks/:dag_name", get(get_default_tasks))
        //
        .route("/graph/:dag_name/:run_id", get(get_run_graph))
        .route("/default_graph/:dag_name", get(get_default_graph))
        .layer(
            CorsLayer::new()
                .allow_methods([Method::GET, Method::POST])
                .allow_origin(Any),
        );

    axum::Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
