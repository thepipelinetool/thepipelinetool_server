use std::{
    collections::HashSet, env, fs, io::ErrorKind, path::PathBuf, process::Command, str::FromStr,
    time::Duration,
};

use chrono::{DateTime, Utc};
use db::Db;
use log::{debug, LevelFilter};
use redis::Connection;
use serde_json::Value;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Pool, Postgres,
};
use thepipelinetool::prelude::*;
use timed::timed;

pub mod catchup;
pub mod db;
pub mod scheduler;

pub const DAGS_DIR: &str = "./bin";

#[timed(duration(printer = "debug!"))]
pub fn _get_default_tasks(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("tasks")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_all_tasks(dag_name: &str, run_id: usize, pool: Pool<Postgres>) -> Vec<Task> {
    let runner = Db::new(dag_name, &[], &HashSet::new(), pool);
    runner.get_all_tasks(&run_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task(dag_name: &str, run_id: usize, task_id: usize, pool: Pool<Postgres>) -> Task {
    let runner = Db::new(dag_name, &[], &HashSet::new(), pool);
    runner.get_task_by_id(&run_id, &task_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_status(
    dag_name: &str,
    run_id: usize,
    task_id: usize,
    pool: Pool<Postgres>,
    // redis: Connection
) -> TaskStatus {
    let mut runner = Db::new(dag_name, &[], &HashSet::new(), pool);
    runner.get_task_status(&run_id, &task_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_result(
    dag_name: &str,
    run_id: usize,
    task_id: usize,
    pool: Pool<Postgres>,
    // redis: Connection
) -> TaskResult {
    let mut runner = Db::new(dag_name, &[], &HashSet::new(), pool);
    runner.get_task_result(&run_id, &task_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_default_edges(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("edges")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_options(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("options")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_dags() -> Vec<String> {
    let paths: Vec<PathBuf> = match fs::read_dir(DAGS_DIR) {
        Err(e) if e.kind() == ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("Unexpected Error! {:?}", e),
        Ok(entries) => entries
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                if path.is_file() {
                    Some(path)
                } else {
                    None
                }
            })
            .collect(),
    };

    paths
        .iter()
        .map(|p| {
            p.file_name()
                .and_then(|os_str| os_str.to_str())
                .unwrap()
                .to_string()
        })
        .collect()
}

#[timed(duration(printer = "debug!"))]
pub fn _trigger_run(dag_name: &str, logical_date: DateTime<Utc>, pool: Pool<Postgres>) {
    let nodes: Vec<Task> = serde_json::from_value(_get_default_tasks(dag_name)).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_value(_get_default_edges(dag_name)).unwrap();

    let hash = hash_dag(
        &serde_json::to_string(&nodes).unwrap(),
        &edges.iter().collect::<Vec<&(usize, usize)>>(),
    );

    Db::new(dag_name, &nodes, &edges, pool.clone()).enqueue_run(dag_name, &hash, logical_date);
}

fn get_db_url() -> String {
    env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:example@0.0.0.0:5432".to_string())
        .to_string()
}

#[timed(duration(printer = "debug!"))]
pub async fn get_client() -> Pool<Postgres> {
    let options = PgConnectOptions::from_str(&get_db_url())
        .unwrap()
        // .log_statements(LevelFilter::Debug);
        .log_slow_statements(LevelFilter::Debug, Duration::new(0, 500_000_000));

    PgPoolOptions::new()
        // .max_connections(max)
        .connect_with(options)
        .await
        .unwrap()
}

fn get_redis_url() -> String {
    env::var("REDIS_URL")
        .unwrap_or("redis://0.0.0.0:6379".to_string())
        .to_string()
}

#[timed(duration(printer = "debug!"))]
pub fn get_redis_client() -> Connection {
    let client = redis::Client::open(get_redis_url()).unwrap();
    client.get_connection().unwrap()
}
