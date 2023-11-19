use std::collections::HashSet;
use std::str::from_utf8;
// use std::str::from_utf8;

use axum::extract::State;
use axum::{extract::Path, http::Method, Json, Router};
use chrono::Utc;
use deadpool_redis::Pool;
use log::debug;

// use runner::{
//     local::{hash_dag, LocalRunner},
//     DefRunner, Runner,
// };
// use runner::{local::hash_dag, DefRunner, Runner};
use serde_json::{json, Value};
use server::{_get_all_task_results, _get_hash, get_redis_pool};
use server::{
    _get_all_tasks, _get_dags, _get_default_edges, _get_default_tasks, _get_options, _get_task,
    _get_task_result, _get_task_status, _trigger_run, redis_runner::RedisRunner,
};
// use sqlx::Pool;
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

    dbg!(1);

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

    Json(json!(v.iter().find(|t| t.id == task_id).unwrap()))
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
    let mut runner = RedisRunner::new("", &[], &HashSet::new(), pool);
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

async fn get_run_graph(Path(run_id): Path<usize>, State(pool): State<Pool>) -> Json<Value> {
    let mut runner = RedisRunner::new("", &[], &HashSet::new(), pool);
    let graph = runner.get_graphite_graph(&run_id);
    json!(graph).into()
}

async fn get_default_graph(Path(dag_name): Path<String>) -> Json<Value> {
    let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_str(&_get_default_edges(&dag_name).await).unwrap();
    let mut runner = InMemoryRunner::new("", &nodes, &edges);
    runner.enqueue_run("local", "", Utc::now());
    let graph = runner.get_graphite_graph(&0);

    json!(graph).into()
}

async fn trigger(Path(dag_name): Path<String>, State(pool): State<Pool>) {
    tokio::spawn(async move {
        _trigger_run(&dag_name, Utc::now(), pool).await;
    });
}

async fn _trigger_local_run(Path(dag_name): Path<String>, State(pool): State<Pool>) {
    let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(&dag_name).await).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_str(&_get_default_edges(&dag_name).await).unwrap();
    let mut runner = RedisRunner::new(&dag_name, &nodes, &edges, pool);
    let dag_run_id = runner.enqueue_run(&dag_name, &_get_hash(&dag_name).await, Utc::now());
    // runner.run(&dag_run_id, 1, Arc::new(Mutex::new(0)));
    // todo!()

    // let tasks = get_tasks().read().unwrap();
    // let edges = get_edges().read().unwrap();
    // let sub_matches = matches.subcommand_matches("local").unwrap();
    // let mode = sub_matches.get_one::<String>("mode").unwrap();

    // let max_threads = max(
    //     usize::from(std::thread::available_parallelism().unwrap()) - 1,
    //     1,
    // );
    // let thread_count = match mode.as_str() {
    //     "--blocking" => 1,
    //     "max" => max_threads,
    //     _ => mode.parse::<usize>().unwrap(),
    // };
    let mut runner = InMemoryRunner::new(&dag_name, &nodes, &edges);
    // let dag_run_id = runner.enqueue_run(&"", &"", Utc::now());
    let default_tasks = runner.get_default_tasks();

    for task in &default_tasks {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let deps = runner.get_circular_dependencies(&dag_run_id, task.id, &mut visited, &mut path);

        if let Some(deps) = deps {
            panic!("{:?}", deps);
        }
    }

    // for task in default_tasks {
    //     // let depth = self.get_task_depth(dag_run_id, &task.id);
    //     // if depth == 0 {
    //     runner.enqueue_task(&dag_run_id, &task.id);
    //     // }
    // }
    // dbg!(2);
    // let runner = Arc::new(Mutex::new(runner));
    // // for _ in 0..thread_count {
    //     dbg!(2);

    //     let runner = runner.clone();
    //     thread::spawn(move || {
    //         let runner = runner.clone();

    while let Some(queued_task) = runner.pop_priority_queue() {
        runner.work(&dag_run_id, queued_task);
        // dbg!(runner.print_priority_queue());

        // if runner.is_completed(&dag_run_id) {
        //     runner.mark_finished(&dag_run_id);
        //     break;
        // }
    }
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    // initialize the logger in the environment? not really sure.
    env_logger::init();

    // let pool: sqlx::Pool<sqlx::Postgres> = get_client().await;
    let pool = get_redis_pool();

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
