use std::{process::Command, fs, io::ErrorKind, path::PathBuf};

use serde_json::Value;

pub mod db;

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

pub fn get_dags() -> Vec<String> {
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