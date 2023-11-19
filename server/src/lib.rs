use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::ErrorKind,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use chrono::{DateTime, Utc};
use deadpool::Runtime;
use deadpool_redis::{Config, Pool};
use log::debug;
use redis_runner::RedisRunner;
use thepipelinetool::prelude::*;
use timed::timed;
use tokio::{process::Command, sync::Mutex};

pub mod catchup;
pub mod redis_runner;
pub mod scheduler;

pub const DAGS_DIR: &str = "./bin";

static TASKS: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();
static HASHES: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();
static EDGES: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();
static OPTIONS: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();

#[timed(duration(printer = "debug!"))]
pub async fn _get_default_tasks(dag_name: &str) -> String {
    let mut tasks = TASKS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock()
        .await;

    if !tasks.contains_key(dag_name) {
        let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
            .arg("tasks")
            .output()
            .await
            .expect("failed to run");

        tasks.insert(
            dag_name.to_owned(),
            String::from_utf8_lossy(&output.stdout).to_string(),
        );
    }

    tasks.get(dag_name).unwrap().to_string()
}

#[timed(duration(printer = "debug!"))]
pub async fn _get_hash(dag_name: &str) -> String {
    let mut hashes = HASHES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock()
        .await;

    if !hashes.contains_key(dag_name) {
        let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
            .arg("hash")
            .output()
            .await
            .expect("failed to run");

        hashes.insert(
            dag_name.to_owned(),
            String::from_utf8_lossy(&output.stdout).to_string(),
        );
    }

    hashes.get(dag_name).unwrap().to_string()
}

#[timed(duration(printer = "debug!"))]
pub async fn _get_default_edges(dag_name: &str) -> String {
    let mut edges = EDGES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock()
        .await;

    if !edges.contains_key(dag_name) {
        let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
            .arg("edges")
            .output()
            .await
            .expect("failed to run");

        edges.insert(
            dag_name.to_owned(),
            String::from_utf8_lossy(&output.stdout).to_string(),
        );
    }

    edges.get(dag_name).unwrap().to_string()
}

#[timed(duration(printer = "debug!"))]
pub async fn _get_options(dag_name: &str) -> String {
    let mut options = OPTIONS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock()
        .await;

    if !options.contains_key(dag_name) {
        let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
            .arg("options")
            .output()
            .await
            .expect("failed to run");

        options.insert(
            dag_name.to_owned(),
            String::from_utf8_lossy(&output.stdout).to_string(),
        );
    }

    options.get(dag_name).unwrap().to_string()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_all_tasks(run_id: usize, pool: Pool) -> Vec<Task> {
    let runner = RedisRunner::new("", &[], &HashSet::new(), pool);
    runner.get_all_tasks(&run_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task(run_id: usize, task_id: usize, pool: Pool) -> Task {
    let runner = RedisRunner::new("", &[], &HashSet::new(), pool);
    runner.get_task_by_id(&run_id, &task_id)
}

#[timed(duration(printer = "debug!"))]
pub async fn _get_all_task_results(run_id: usize, task_id: usize, pool: Pool) -> Vec<TaskResult> {
    RedisRunner::get_all_results(run_id, task_id, pool).await
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_status(
    run_id: usize,
    task_id: usize,
    pool: Pool,
    // redis: Connection
) -> TaskStatus {
    let mut runner = RedisRunner::new("", &[], &HashSet::new(), pool);
    runner.get_task_status(&run_id, &task_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_result(
    run_id: usize,
    task_id: usize,
    pool: Pool,
    // redis: Connection
) -> TaskResult {
    let mut runner = RedisRunner::new("", &[], &HashSet::new(), pool);
    runner.get_task_result(&run_id, &task_id)
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
pub async fn _trigger_run(dag_name: &str, logical_date: DateTime<Utc>, pool: Pool) {
    let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(dag_name).await).unwrap();
    let edges: HashSet<(usize, usize)> =
        serde_json::from_str(&_get_default_edges(dag_name).await).unwrap();
    let hash = _get_hash(dag_name).await;

    RedisRunner::new(dag_name, &nodes, &edges, pool.clone()).enqueue_run(
        dag_name,
        &hash,
        logical_date,
    );
}

fn get_redis_url() -> String {
    env::var("REDIS_URL")
        .unwrap_or("redis://0.0.0.0:6379".to_string())
        .to_string()
}

// #[timed(duration(printer = "debug!"))]
pub fn get_redis_pool() -> Pool {
    let cfg = Config::from_url(get_redis_url());
    cfg.create_pool(Some(Runtime::Tokio1)).unwrap()
}

// #[macro_export]
// macro_rules! transaction_async {
//     ($conn:expr, $keys:expr, $body:expr) => {
//         loop {
//             redis::cmd("WATCH")
//                 .arg($keys)
//                 .query_async::<_, String>($conn)
//                 .await
//                 .unwrap();

//             if let Some(response) = $body {
//                 redis::cmd("UNWATCH")
//                     .query_async::<_, String>($conn)
//                     .await
//                     .unwrap();
//                 break response;
//             }
//         }
//     };
// }
