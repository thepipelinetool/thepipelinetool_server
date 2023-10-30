use std::collections::HashSet;

use thepipelinetool::prelude::*;

use server::{_get_edges, _get_tasks, db::Db, get_dags};

#[tokio::main]
async fn main() {
    loop {
        for dag_name in get_dags() {
            let all_runs = Db::get_all_runs(&dag_name).await;

            for (run_id, dag_id) in all_runs {
                let nodes: Vec<Task> = serde_json::from_value(_get_tasks(&dag_name)).unwrap();
                let edges: HashSet<(usize, usize)> =
                    serde_json::from_value(_get_edges(&dag_name)).unwrap();

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
