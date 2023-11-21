use std::{env, fs, io::ErrorKind, path::PathBuf};

use chrono::{DateTime, Utc};
use deadpool::Runtime;
use deadpool_redis::{Config, Pool};
use log::debug;
use redis_runner::RedisRunner;
use thepipelinetool::prelude::*;
use timed::timed;

use crate::statics::_get_hash;

pub mod catchup;
pub mod redis_runner;
pub mod scheduler;
pub mod statics;
pub mod check_timeout;

pub const DAGS_DIR: &str = "./bin";

#[timed(duration(printer = "debug!"))]
pub fn _get_all_tasks(run_id: usize, pool: Pool) -> Vec<Task> {
    RedisRunner::dummy(pool).get_all_tasks(run_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task(run_id: usize, task_id: usize, pool: Pool) -> Task {
    RedisRunner::dummy(pool).get_task_by_id(run_id, task_id)
}

#[timed(duration(printer = "debug!"))]
pub async fn _get_all_task_results(run_id: usize, task_id: usize, pool: Pool) -> Vec<TaskResult> {
    RedisRunner::get_all_results(run_id, task_id, pool).await
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_status(run_id: usize, task_id: usize, pool: Pool) -> TaskStatus {
    RedisRunner::dummy(pool).get_task_status(run_id, task_id)
}

#[timed(duration(printer = "debug!"))]
pub fn _get_task_result(run_id: usize, task_id: usize, pool: Pool) -> TaskResult {
    RedisRunner::dummy(pool).get_task_result(run_id, task_id)
}

// TODO cache response to prevent disk read
#[timed(duration(printer = "debug!"))]
pub fn _get_dags() -> Vec<String> {
    let paths: Vec<PathBuf> = match fs::read_dir(DAGS_DIR) {
        Err(e) if e.kind() == ErrorKind::NotFound => vec![],
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
    let hash = _get_hash(dag_name);

    RedisRunner::from_local_dag(dag_name, pool.clone()).enqueue_run(dag_name, &hash, logical_date);
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
