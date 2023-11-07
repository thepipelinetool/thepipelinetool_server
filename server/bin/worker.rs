use std::collections::HashSet;

// use thepipelinetool::prelude::*;

use runner::{local::hash_dag, Runner, DefRunner};
use server::{_get_default_edges, _get_default_tasks, _get_dags, db::Db};
use task::task::Task;

#[tokio::main]
async fn main() {
    loop {
        for dag_name in _get_dags() {
            let all_runs = Db::get_all_runs(&dag_name).await;

            for (run_id, dag_id) in all_runs {
                let nodes: Vec<Task> = serde_json::from_value(_get_default_tasks(&dag_name)).unwrap();
                let edges: HashSet<(usize, usize)> =
                    serde_json::from_value(_get_default_edges(&dag_name)).unwrap();

                let hash = hash_dag(
                    &serde_json::to_string(&nodes).unwrap(),
                    &edges.iter().collect::<Vec<&(usize, usize)>>(),
                );

                // dbg!(run_id, &dag_id, format!("{DAGS_DIR}/{dag_name}"));

                if dag_id == hash {
                    Db::new(&dag_name, &nodes, &edges).run(&run_id, 1);
                }
            }
        }

        // thread::sleep(Duration::new(1, 0));
    }
}
