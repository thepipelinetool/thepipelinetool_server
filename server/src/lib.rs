use std::{process::Command, fs, io::ErrorKind, path::PathBuf, collections::HashSet};

use chrono::{DateTime, Utc};
use db::Db;
use runner::{local::hash_dag, Runner, DefRunner};
use serde_json::Value;
use task::task::Task;

pub mod db;
pub mod catchup;
pub mod scheduler;

pub const DAGS_DIR: &str = "./bin";

pub fn _get_tasks(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("tasks")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

pub fn _get_edges(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("edges")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

pub fn _get_options(dag_name: &str) -> Value {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("options")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(result_raw.as_ref()).unwrap()
}

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

pub fn _trigger_run(dag_name: &str, logical_date: DateTime<Utc>) {
    let nodes: Vec<Task> = serde_json::from_value(_get_tasks(&dag_name)).unwrap();
    let edges: HashSet<(usize, usize)> = serde_json::from_value(_get_edges(&dag_name)).unwrap();

    let hash = hash_dag(
        &serde_json::to_string(&nodes).unwrap(),
        &edges.iter().collect::<Vec<&(usize, usize)>>(),
    );

    Db::new(&dag_name, &nodes, &edges).enqueue_run(&dag_name, &hash, logical_date);
}