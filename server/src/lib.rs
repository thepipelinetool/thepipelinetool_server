use std::{env, fs, io::ErrorKind, path::PathBuf};

use chrono::{DateTime, Utc};
use deadpool::Runtime;
use deadpool_redis::{Config, Pool};
use log::{debug, info};
use redis_runner::{RedisRunner, Run};
use saffron::Cron;
use thepipelinetool::prelude::*;
use timed::timed;

use crate::statics::{_get_hash, _get_options};

pub mod options;
pub mod catchup;
pub mod check_timeout;
pub mod redis_runner;
pub mod scheduler;
pub mod statics;

pub fn get_dags_dir() -> String {
    env::var("DAGS_DIR")
        .unwrap_or("./bin".to_string())
        .to_string()
}

fn get_redis_url() -> String {
    env::var("REDIS_URL")
        .unwrap_or("redis://0.0.0.0:6379".to_string())
        .to_string()
}

pub fn _get_dag_path_by_name(dag_name: &str) -> PathBuf {
    let dags_dir = &get_dags_dir();
    [dags_dir, dag_name].iter().collect()
}

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
    let paths: Vec<PathBuf> = match fs::read_dir(get_dags_dir()) {
        Err(e) if e.kind() == ErrorKind::NotFound => vec![],
        Err(e) => panic!("Unexpected Error! {:?}", e),
        Ok(entries) => entries
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                if path.is_file() && path.extension().is_none() {
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

pub fn _get_next_run(dag_name: &str) -> Vec<Value> {
    let options = _get_options(&dag_name);

    info!("{:#?}", options);

    if let Some(schedule) = &options.schedule {

        match schedule.parse::<Cron>() {
            Ok(cron) => {
                if !cron.any() {
                    info!("Cron will never match any given time!");
                    return vec![];
                }

                if let Some(start_date) = options.start_date {
                    info!("Start date: {start_date}");
                } else {
                    info!("Start date: None");
                }

                info!("Upcoming:");
                let futures =
                    cron.clone()
                        .iter_from(if let Some(start_date) = options.start_date {
                            if options.catchup || start_date > Utc::now() {
                                start_date.into()
                            } else {
                                Utc::now()
                            }
                        } else {
                            Utc::now()
                        });
                let mut next_runs = vec![];
                for time in futures.take(1) {
                    if !cron.contains(time) {
                        info!("Failed check! Cron does not contain {}.", time);
                        break;
                    }
                    if let Some(end_date) = options.end_date {
                        if time > end_date {
                            break;
                        }
                    }
                    next_runs.push(json!({
                        "date": format!("{}", time.format("%F %R"))
                    }));
                    info!("  {}", time.format("%F %R"));
                }

                return next_runs;
            }
            Err(err) => info!("{err}: {schedule}"),
        }
    }

    vec![]
}

pub async fn _get_last_run(dag_name: &str, pool: Pool) -> Vec<Run> {
    let r = RedisRunner::get_last_run(&dag_name, pool).await;

    match r {
        Some(run) => vec![run],
        None => vec![],
    }
}

pub async fn _get_recent_runs(dag_name: &str, pool: Pool) -> Vec<Run> {
    RedisRunner::get_recent_runs(&dag_name, pool).await
}