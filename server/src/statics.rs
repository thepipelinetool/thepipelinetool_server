use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Command,
    sync::{Arc, OnceLock},
};

use log::debug;
use parking_lot::Mutex;
use thepipelinetool::server::Task;
use thepipelinetool_utils::{value_from_file, value_to_file};
use timed::timed;

use crate::{_get_dag_path_by_name, get_dags_dir, options::DagOptions};

static TASKS: OnceLock<Arc<Mutex<HashMap<String, Vec<Task>>>>> = OnceLock::new();
static HASHES: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();
static EDGES: OnceLock<Arc<Mutex<HashMap<String, HashSet<(usize, usize)>>>>> = OnceLock::new();
static DAG_OPTIONS: OnceLock<Arc<Mutex<HashMap<String, DagOptions>>>> = OnceLock::new();

#[timed(duration(printer = "debug!"))]
pub fn _get_default_tasks(dag_name: &str) -> Vec<Task> {
    let mut tasks = TASKS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock();

    if !tasks.contains_key(dag_name) {
        let output = Command::new(_get_dag_path_by_name(dag_name))
            .arg("tasks")
            .output()
            .expect("failed to run");

        tasks.insert(
            dag_name.to_owned(),
            serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap(),
        );
    }

    tasks.get(dag_name).unwrap().clone()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_hash(dag_name: &str) -> String {
    let mut hashes = HASHES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock();

    if !hashes.contains_key(dag_name) {
        let dags_dir = &get_dags_dir();
        let path: PathBuf = [dags_dir, dag_name].iter().collect();
        let output = Command::new(path)
            .arg("hash")
            .output()
            .expect("failed to run");

        hashes.insert(
            dag_name.to_owned(),
            String::from_utf8_lossy(&output.stdout).to_string(),
        );
    }

    hashes.get(dag_name).unwrap().to_string()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_default_edges(dag_name: &str) -> HashSet<(usize, usize)> {
    let mut edges = EDGES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock();

    if !edges.contains_key(dag_name) {
        let output = Command::new(_get_dag_path_by_name(dag_name))
            .arg("edges")
            .output()
            .expect("failed to run");

        edges.insert(
            dag_name.to_owned(),
            serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap(),
        );
    }

    edges.get(dag_name).unwrap().clone()
}

#[timed(duration(printer = "debug!"))]
pub fn _get_options(dag_name: &str) -> DagOptions {
    let mut dag_options = DAG_OPTIONS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock();

    if !dag_options.contains_key(dag_name) {
        // let output = Command::new(_get_dag_path_by_name(dag_name))
        //     .arg("options")
        //     .output()
        //     .expect("failed to run");
        // dbg!(&dag_name);
        let mut path = _get_dag_path_by_name(dag_name);
        path.set_extension("json");

        if let Ok(options) = value_from_file::<DagOptions>(&path) {
            dag_options.insert(dag_name.to_owned(), options);
        } else {
            dag_options.insert(dag_name.to_owned(), DagOptions::default());
        }

        // dbg!(&String::from_utf8_lossy(&output.stdout));
    }

    dag_options.get(dag_name).unwrap().clone()
}

#[timed(duration(printer = "debug!"))]
pub fn _set_options(dag_name: &str, options: DagOptions) {
    let mut path = _get_dag_path_by_name(dag_name);
    path.set_extension("json");
    value_to_file(&options, &path);

    let mut dag_options = DAG_OPTIONS
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .lock();

    dag_options.remove(dag_name);
}
