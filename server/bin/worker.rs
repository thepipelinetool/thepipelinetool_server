use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

// use thepipelinetool::prelude::*;

// use runner::{local::hash_dag, DefRunner, Runner};
use server::{
    _get_dags, _get_default_edges, _get_default_tasks, _get_hash, get_redis_pool,
    redis_runner::RedisRunner,
};
use thepipelinetool::prelude::*;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    // initialize the logger in the environment? not really sure.
    env_logger::init();
    let pool = get_redis_pool();

    let mut node_hashmap: HashMap<String, Vec<Task>> = HashMap::new();
    let mut edges_hashmap: HashMap<String, HashSet<(usize, usize)>> = HashMap::new();
    let mut hash_hashmap: HashMap<String, String> = HashMap::new();
    let dags = &_get_dags();

    for dag_name in dags {
        let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(dag_name).await).unwrap();
        let edges: HashSet<(usize, usize)> =
            serde_json::from_str(&_get_default_edges(dag_name).await).unwrap();

        hash_hashmap.insert(dag_name.clone(), _get_hash(dag_name).await.to_string());
        node_hashmap.insert(dag_name.clone(), nodes);
        edges_hashmap.insert(dag_name.clone(), edges);
    }
    let mut dummy = RedisRunner::new("", &[], &HashSet::new(), pool.clone());

    loop {
        if let Some(ordered_queued_task) = dummy.pop_priority_queue() {
            let dag_name = &ordered_queued_task.task.dag_name;
            let run_id = ordered_queued_task.task.run_id;

            let nodes = node_hashmap.get(dag_name).unwrap();
            let edges = edges_hashmap.get(dag_name).unwrap();
            let mut runner = RedisRunner::new(dag_name, nodes, edges, pool.clone());
            runner.work(&run_id, ordered_queued_task);
        } else {
            sleep(Duration::new(2, 0)).await;
        }
    }
}
