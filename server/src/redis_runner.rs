use deadpool_redis::{redis::cmd, Pool};
use log::debug;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use std::str::FromStr;
use thepipelinetool::prelude::*;

pub struct RedisRunner {
    edges: HashSet<(usize, usize)>,
    nodes: Vec<Task>,
    name: String,
    pool: Pool,
}

use timed::timed;

#[derive(Serialize, Deserialize)]
pub struct Run {
    pub run_id: usize,
    pub date: DateTime<Utc>,
}

const TASK_STATUS_KEY: &str = "ts";
const TASK_RESULTS_KEY: &str = "trs";
const RUNS_KEY: &str = "runs";
const LOGICAL_DATES_KEY: &str = "ld";
const DEPTH_KEY: &str = "d";
const TASK_RESULT_KEY: &str = "tr";
const LOG_KEY: &str = "l";
const TASK_ATTEMPT_KEY: &str = "a";
const DEPENDENCY_KEYS_KEY: &str = "dk";
const EDGES_KEY: &str = "e";
const TASKS_KEY: &str = "tks";
const TASK_ID_KEY: &str = "ti";
const TASK_KEY: &str = "t";
const TEMPLATE_ARGS_KEY: &str = "ta";

impl RedisRunner {
    #[timed(duration(printer = "debug!"))]
    pub fn new(name: &str, nodes: &[Task], edges: &HashSet<(usize, usize)>, pool: Pool) -> Self {
        Self {
            name: name.into(),
            edges: edges.clone(),
            nodes: nodes.to_vec(),
            pool,
        }
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn get_all_results(run_id: usize, task_id: usize, pool: Pool) -> Vec<TaskResult> {
        let mut conn = pool.get().await.unwrap();
        dbg!(cmd("LRANGE")
            .arg(&format!("{TASK_RESULTS_KEY}:{run_id}:{task_id}"))
            .arg(0)
            .arg(-1)
            .query_async::<_, Vec<String>>(&mut conn)
            .await
            .unwrap())
        .iter()
        .map(|v| serde_json::from_str(v).unwrap())
        .collect()
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn get_runs(dag_name: &str, pool: Pool) -> Vec<Run> {
        let mut conn = pool.get().await.unwrap();
        dbg!(cmd("LRANGE")
            .arg(&format!("{RUNS_KEY}:{dag_name}"))
            .arg(0)
            .arg(-1)
            .query_async::<_, Vec<String>>(&mut conn)
            .await
            .unwrap())
        .iter()
        .map(|v| serde_json::from_str(v).unwrap())
        .collect()
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn contains_logical_date(
        dag_name: &str,
        dag_hash: &str,
        logical_date: DateTime<Utc>,
        pool: Pool,
    ) -> bool {
        // TODO remove block
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = pool.get().await.unwrap();
                cmd("SISMEMBER")
                    .arg(&[
                        format!("{LOGICAL_DATES_KEY}:{dag_name}:{dag_hash}"),
                        logical_date.to_string(),
                    ])
                    .query_async::<_, bool>(&mut conn)
                    .await
                    .unwrap()
            })
        })
    }
}

impl Runner for RedisRunner {
    fn delete_task_depth(&mut self, dag_run_id: &usize, task_id: &usize) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                cmd("DEL")
                    .arg(&format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"))
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();
            });
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn get_log(&mut self, dag_run_id: &usize, task_id: &usize, attempt: usize) -> String {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("LRANGE")
                    .arg(&[format!("{LOG_KEY}:{dag_run_id}:{task_id}{attempt}")])
                    .arg(0)
                    .arg(-1)
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await
                    .unwrap_or_default()
                    .join("\n")
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn handle_log(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        attempt: usize,
    ) -> Box<dyn Fn(String) + Send> {
        let task_id = *task_id;
        let dag_run_id = *dag_run_id;
        let pool = self.pool.clone();

        Box::new(move |s| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let mut conn = pool.get().await.unwrap();
                cmd("RPUSH")
                    .arg(&dbg!(format!("{LOG_KEY}:{dag_run_id}:{task_id}:{attempt}")))
                    .arg(s)
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
            });
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_dag_name(&self) -> String {
        self.name.clone()
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_result(&mut self, dag_run_id: &usize, task_id: &usize) -> TaskResult {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                serde_json::from_str(
                    &cmd("GET")
                        .arg(&[dbg!(format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"))])
                        .query_async::<_, String>(&mut conn)
                        .await
                        .unwrap(),
                )
                .unwrap()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_attempt_by_task_id(&self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                cmd("INCR")
                    .arg(&[format!("{TASK_ATTEMPT_KEY}:{dag_run_id}:{task_id}")])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_status(&mut self, dag_run_id: &usize, task_id: &usize) -> TaskStatus {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                TaskStatus::from_str(
                    &cmd("GET")
                        .arg(&[dbg!(format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}"))])
                        .query_async::<_, String>(&mut conn)
                        .await
                        .unwrap(),
                )
                .unwrap()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_task_status(&mut self, dag_run_id: &usize, task_id: &usize, task_status: TaskStatus) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("SET")
                    .arg(&[
                        dbg!(format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}")),
                        task_status.as_str().to_string(),
                    ])
                    .query_async::<_, String>(&mut conn)
                    .await
                    .unwrap();
            });
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn create_new_run(
        &mut self,
        dag_name: &str,
        dag_hash: &str,
        logical_date: DateTime<Utc>,
    ) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                let run_id = cmd("INCR")
                    .arg("run")
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();

                let mut conn = self.pool.get().await.unwrap();
                cmd("RPUSH")
                    .arg(&format!("{RUNS_KEY}:{dag_name}"))
                    .arg(
                        serde_json::to_string(&Run {
                            run_id,
                            date: logical_date,
                        })
                        .unwrap(),
                    )
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
                run_id
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_task_results(&mut self, dag_run_id: &usize, result: &TaskResult) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                let res = serde_json::to_string(result).unwrap();
                let task_id = result.task_id;

                cmd("RPUSH")
                    .arg(&[
                        format!("{TASK_RESULTS_KEY}:{dag_run_id}:{task_id}"),
                        res.to_string(),
                    ])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
                cmd("SET")
                    .arg(&[format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"), res])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn any_upstream_incomplete(&mut self, dag_run_id: &usize, task_id: &usize) -> bool {
        self.get_upstream(dag_run_id, task_id)
            .iter()
            .any(|edge| !self.is_task_completed(dag_run_id, edge))
    }

    #[timed(duration(printer = "debug!"))]
    fn get_dependency_keys(
        &self,
        dag_run_id: &usize,
        task_id: &usize,
    ) -> HashMap<(usize, String), String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                let k: Vec<((usize, String), String)> = cmd("SMEMBERS")
                    .arg(&dbg!(format!(
                        "{DEPENDENCY_KEYS_KEY}:{dag_run_id}:{task_id}"
                    )))
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|v| serde_json::from_str(v).unwrap())
                    .collect();
                k.into_iter().collect()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_dependency_keys(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        upstream: (usize, String),
        v: String,
    ) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("SADD")
                    .arg(&format!("{DEPENDENCY_KEYS_KEY}:{dag_run_id}:{task_id}"))
                    .arg(dbg!(serde_json::to_string(&(upstream, v)).unwrap()))
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_downstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                dbg!(HashSet::from_iter(
                    cmd("SMEMBERS")
                        .arg(&[format!("{EDGES_KEY}:{dag_run_id}")])
                        .query_async::<_, Vec<String>>(&mut conn)
                        .await
                        .unwrap()
                        .iter()
                        .map(|f| {
                            let v: (usize, usize) = serde_json::from_str(f).unwrap();
                            v
                        })
                        .filter_map(|(up, down)| {
                            if up == *task_id {
                                return Some(down);
                            }
                            None
                        })
                ))
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_upstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                dbg!(HashSet::from_iter(
                    cmd("SMEMBERS")
                        .arg(&[format!("{EDGES_KEY}:{dag_run_id}")])
                        .query_async::<_, Vec<String>>(&mut conn)
                        .await
                        .unwrap()
                        .iter()
                        .map(|f| {
                            let v: (usize, usize) = serde_json::from_str(f).unwrap();
                            v
                        })
                        .filter_map(|(up, down)| {
                            if down == *task_id {
                                return Some(up);
                            }
                            None
                        })
                ))
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn remove_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("SREM")
                    .arg(&[
                        format!("{EDGES_KEY}:{dag_run_id}"),
                        serde_json::to_string(edge).unwrap(),
                    ])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();
                cmd("SREM")
                    .arg(&format!("{DEPENDENCY_KEYS_KEY}:{dag_run_id}:{}", edge.1))
                    .arg(dbg!(serde_json::to_string(&(edge.0, "")).unwrap()))
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("SADD")
                    .arg(&[
                        format!("{EDGES_KEY}:{dag_run_id}"),
                        serde_json::to_string(edge).unwrap(),
                    ])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_default_tasks(&self) -> Vec<Task> {
        self.nodes.clone()
    }

    #[timed(duration(printer = "debug!"))]
    fn get_all_tasks(&self, dag_run_id: &usize) -> Vec<Task> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn: deadpool_redis::Connection = self.pool.get().await.unwrap();
                cmd("SMEMBERS")
                    .arg(&format!("{TASKS_KEY}:{dag_run_id}"))
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await
                    .unwrap()
                    .iter()
                    .map(|t| serde_json::from_str(t).unwrap())
                    .collect()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_default_edges(&self) -> HashSet<(usize, usize)> {
        self.edges.clone()
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_by_id(&self, dag_run_id: &usize, task_id: &usize) -> Task {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                serde_json::from_str(
                    &cmd("GET")
                        .arg(&[format!("{TASK_KEY}:{dag_run_id}:{task_id}")])
                        .query_async::<_, String>(&mut conn)
                        .await
                        .unwrap(),
                )
                .unwrap()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn append_new_task_and_set_status_to_pending(
        &mut self,
        dag_run_id: &usize,
        function_name: String,
        template_args: serde_json::Value,
        options: TaskOptions,
        lazy_expand: bool,
        is_dynamic: bool,
        is_branch: bool,
    ) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                let task_id = cmd("INCR")
                    .arg(&[format!("{TASK_ID_KEY}:{dag_run_id}")])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
                    - 1;

                let task = Task {
                    id: task_id,
                    function_name,
                    template_args,
                    options,
                    lazy_expand,
                    is_dynamic,
                    is_branch,
                };
                cmd("SADD")
                    .arg(&[
                        format!("{TASKS_KEY}:{dag_run_id}"),
                        serde_json::to_string(&task).unwrap(),
                    ])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();
                cmd("SET")
                    .arg(&[
                        format!("{TASK_KEY}:{dag_run_id}:{task_id}"),
                        serde_json::to_string(&task).unwrap(),
                    ])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
                dbg!(dag_run_id, task_id);
                cmd("SET")
                    .arg(&[
                        format!("{TEMPLATE_ARGS_KEY}:{dag_run_id}:{task_id}"),
                        serde_json::to_string(&task.template_args).unwrap(),
                    ])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
                self.set_task_status(dag_run_id, &task_id, TaskStatus::Pending);
                task_id
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_template_args(&self, dag_run_id: &usize, task_id: &usize) -> serde_json::Value {
        let task = self.get_task_by_id(dag_run_id, task_id);
        task.template_args
    }

    #[timed(duration(printer = "debug!"))]
    fn set_template_args(&mut self, dag_run_id: &usize, task_id: &usize, template_args_str: &str) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                let mut task = self.get_task_by_id(dag_run_id, task_id);
                task.template_args = serde_json::from_str(template_args_str).unwrap();

                cmd("SET")
                    .arg(&[
                        format!("{TASK_KEY}:{dag_run_id}:{task_id}"),
                        serde_json::to_string(&task).unwrap(),
                    ])
                    .query_async::<_, String>(&mut conn)
                    .await
                    .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn pop_priority_queue(&mut self) -> Option<OrderedQueuedTask> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                let res = cmd("ZPOPMIN")
                    .arg(&["queue".to_string(), "1".to_string()]) // TODO timeout arg
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await;

                if let Ok(vec) = &res {
                    if !vec.is_empty() {
                        dbg!(&vec);

                        cmd("SADD")
                            .arg(&["tmpqueue".to_string(), vec[0].to_string()])
                            .query_async::<_, ()>(&mut conn)
                            .await
                            .unwrap();
                        return Some(OrderedQueuedTask {
                            score: vec[1].parse().unwrap(),
                            task: serde_json::from_str(&vec[0]).unwrap(),
                        });
                    }
                } else {
                    println!("{:#?}", res.unwrap_err().detail());
                }

                None
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn push_priority_queue(&mut self, queued_task: OrderedQueuedTask) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();
                cmd("ZADD")
                    .arg(&format!("queue"))
                    .arg(dbg!(queued_task.score))
                    .arg(dbg!(serde_json::to_string(&queued_task.task).unwrap()))
                    .query_async::<_, f64>(&mut conn)
                    .await
                    .unwrap();
            });
        });
    }

    fn get_task_depth(&mut self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                if !cmd("EXISTS")
                    .arg(&format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"))
                    .query_async::<_, bool>(&mut conn)
                    .await
                    .unwrap()
                {
                    let depth = self
                        .get_upstream(dag_run_id, task_id)
                        .iter()
                        .map(|up| self.get_task_depth(dag_run_id, up) + 1)
                        .max()
                        .unwrap_or(0);
                    self.set_task_depth(dag_run_id, task_id, depth);
                }
                cmd("GET")
                    .arg(&format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"))
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_task_depth(&mut self, dag_run_id: &usize, task_id: &usize, depth: usize) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                cmd("SET")
                    .arg(&[
                        format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"),
                        depth.to_string(),
                    ])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            });
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn enqueue_task(&mut self, dag_run_id: &usize, task_id: &usize) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let depth = self.get_task_depth(dag_run_id, task_id);
                let mut conn = self.pool.get().await.unwrap();

                cmd("ZADD")
                    .arg(&[
                        "queue".to_string(),
                        depth.to_string(),
                        serde_json::to_string(&QueuedTask {
                            // depth,
                            task_id: *task_id,
                            run_id: *dag_run_id,
                            dag_name: self.get_dag_name(),
                        })
                        .unwrap(),
                    ])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();
            });
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn print_priority_queue(&mut self) {}
}
