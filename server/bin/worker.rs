use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

// use thepipelinetool::prelude::*;

// use runner::{local::hash_dag, DefRunner, Runner};
use server::{_get_dags, _get_default_edges, _get_default_tasks, _get_hash, db::Db, get_client};
use thepipelinetool::prelude::*;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    // initialize the logger in the environment? not really sure.
    env_logger::init();

    let mut node_hashmap: HashMap<String, Vec<Task>> = HashMap::new();
    let mut edges_hashmap: HashMap<String, HashSet<(usize, usize)>> = HashMap::new();
    let mut hash_hashmap: HashMap<String, String> = HashMap::new();
    let pool: sqlx::Pool<sqlx::Postgres> = get_client().await;
    let dags = &_get_dags();

    for dag_name in dags {
        // let all_runs = Db::get_all_runs(dag_name, pool.clone()).await;

        // for (run_id, dag_id) in all_runs {
        let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(dag_name).await).unwrap();
        let edges: HashSet<(usize, usize)> =
            serde_json::from_str(&_get_default_edges(dag_name).await).unwrap();
        // dbg!(run_id, &dag_id);

        hash_hashmap.insert(dag_name.clone(), _get_hash(&dag_name).await.to_string());

        node_hashmap.insert(dag_name.clone(), nodes);
        edges_hashmap.insert(dag_name.clone(), edges);
        // }
    }
    loop {
        for dag_name in dags {
            let all_runs = Db::get_pending_runs(dag_name, pool.clone()).await;

            for (run_id, dag_id) in all_runs {
                // dbg!(run_id, &dag_id);
                let nodes = node_hashmap.get(dag_name).unwrap();
                // .or_insert(serde_json::from_value(_get_default_tasks(&dag_name)).unwrap())
                // .to_vec();
                let edges: &HashSet<(usize, usize)> = edges_hashmap.get(dag_name).unwrap();

                let hash = hash_hashmap.get(dag_name).unwrap();

                // dbg!(run_id, &dag_id, format!("{DAGS_DIR}/{dag_name}"));

                // TODO read max_threads from env
                if &dag_id == hash {
                    Db::new(dag_name, nodes, edges, pool.clone()).run(&run_id, 9);
                }
            }
        }

        sleep(Duration::new(5, 0)).await;
    }
}
