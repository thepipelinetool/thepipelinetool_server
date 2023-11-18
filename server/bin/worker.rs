use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

// use thepipelinetool::prelude::*;

// use runner::{local::hash_dag, DefRunner, Runner};
use server::{_get_dags, _get_default_edges, _get_default_tasks, _get_hash, redis_runner::RedisRunner, get_redis_pool};
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
    // let pool: sqlx::Pool<sqlx::Postgres> = get_client().await;
    let dags = &_get_dags();

    for dag_name in dags {
        // let all_runs = Db::get_all_runs(dag_name, pool.clone()).await;

        // for (run_id, dag_id) in all_runs {
        let nodes: Vec<Task> = serde_json::from_str(&_get_default_tasks(dag_name).await).unwrap();
        let edges: HashSet<(usize, usize)> =
            serde_json::from_str(&_get_default_edges(dag_name).await).unwrap();
        // dbg!(run_id, &dag_id);

        hash_hashmap.insert(dag_name.clone(), _get_hash(dag_name).await.to_string());

        node_hashmap.insert(dag_name.clone(), nodes);
        edges_hashmap.insert(dag_name.clone(), edges);
        // }
    }
    let mut dummy = RedisRunner::new("", &[], &HashSet::new(), pool.clone());

    // loop {
    //     // let pool = pool.clone();
    //     dbg!(2);
        // let runner = runner.clone();

        // let mut runner = runner.lock().unwrap();
        // dbg!(2);

        // runner.lock().unwrap().run_dag_local();
        if let Some(ordered_queued_task) = dummy.pop_priority_queue() {
            // let (depth, queued_task) = (ordered_queued_task.score, ordered_queued_task.task);
            let dag_name = &ordered_queued_task.task.dag_name;
            let run_id = ordered_queued_task.task.run_id;

            let nodes = node_hashmap.get(dag_name).unwrap();
            // .or_insert(serde_json::from_value(_get_default_tasks(&dag_name)).unwrap())
            // .to_vec();
            let edges = edges_hashmap.get(dag_name).unwrap();
            dbg!(&dag_name, nodes.len(), edges.len());
            let mut runner = RedisRunner::new(dag_name, nodes, edges, pool.clone());
            // dbg!(1);
            runner.work(&run_id, ordered_queued_task);

            // if runner.is_completed(&run_id) {
            //     runner.mark_finished(&run_id);
            // }
        } else {
            sleep(Duration::new(2, 0)).await;
        }
            // break;
        

    // }
    // dbg!(10);


    // todo!();
    // loop {
    //     for dag_name in dags {
    //         let all_runs = Db::get_pending_runs(dag_name, pool.clone()).await;

    //         for (run_id, dag_id) in all_runs {
    //             // dbg!(run_id, &dag_id);
    //             let nodes = node_hashmap.get(dag_name).unwrap();
    //             // .or_insert(serde_json::from_value(_get_default_tasks(&dag_name)).unwrap())
    //             // .to_vec();
    //             let edges: &HashSet<(usize, usize)> = edges_hashmap.get(dag_name).unwrap();

    //             let hash = hash_hashmap.get(dag_name).unwrap();

    //             // dbg!(run_id, &dag_id, format!("{DAGS_DIR}/{dag_name}"));

    //             // TODO read max_threads from env
    //             if &dag_id == hash {
    //                 Db::new(dag_name, nodes, edges, pool.clone()).run(&run_id, 9, Arc::new(Mutex::new(0)));
    //             }
    //         }
    //     }

    //     sleep(Duration::new(5, 0)).await;
    // }
}
