use deadpool_redis::{redis::cmd, Pool};
use log::debug;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
// use redis::{Commands, Connection};
// use serde_json::Value;
use std::str::FromStr;
use thepipelinetool::prelude::*;

// use task::task_options::TaskOptions;
// use task::{task::Task, task_result::TaskResult, {TASK_STATUS_KEY}::TaskStatus};

// #[derive(Clone)]
pub struct RedisRunner {
    edges: HashSet<(usize, usize)>,
    nodes: Vec<Task>,
    name: String,
    // pool: Pool,
    pool: Pool,
}

// impl Default for Postgres {
//     fn default() -> Self {
//         Self {}
//     }
// }

// use sqlx::{Pool, Postgres, Row};

use timed::timed;

// use task::task::Task;
// use task::task_options::TaskOptions;
// use task::task_result::TaskResult;
// use task::{TASK_STATUS_KEY}::TaskStatus;
// use thepipelinetool::prelude::*;

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
    pub fn new(
        name: &str,
        nodes: &[Task],
        edges: &HashSet<(usize, usize)>,
        pool: Pool,
        // redis: Connection,
    ) -> Self {
        // let redis = get_redis_pool();
        Self {
            name: name.into(),
            edges: edges.clone(),
            nodes: nodes.to_vec(),
            pool,
        }
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn get_all_results(run_id: usize, task_id: usize, pool: Pool) -> Vec<TaskResult> {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //         //

        //         let tasks = self.get_
        //     })
        // })

        //    let db =  Db::new(&"", &[], &HashSet::new(), pool);

        //    db.get_a

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

        // let rows = sqlx::query(
        //     "
        //     SELECT *
        //     FROM task_results
        //     WHERE run_id = $1 AND task_id = $2
        //     ORDER BY timestamp DESC;
        //     ",
        // )
        // .bind(run_id as i32)
        // .bind(task_id as i32)
        // .fetch_all(&pool)
        // .await
        // .unwrap();
        // let id: i32 = task.get("task_id");
        // let name: String = task.get("name");
        // let function_name: String = task.get("function_name");
        // let template_args: Value =
        //     serde_json::from_value(task.get("template_args")).unwrap();
        // let options: TaskOptions = serde_json::from_value(task.get("options")).unwrap();
        // let lazy_expand: bool = task.get("lazy_expand");
        // let is_dynamic: bool = task.get("is_dynamic");

        // let mut results = vec![];
        // for row in rows {
        //     results.push(TaskResult {
        //         task_id: row.get::<i32, _>("task_id") as usize,
        //         result: serde_json::from_value(row.get("result")).unwrap(),
        //         // task_name: task.get("task_name"),
        //         attempt: row.get::<i32, _>("attempt") as usize,
        //         max_attempts: row.get::<i32, _>("max_attempts") as isize,
        //         function_name: row.get("function_name"),
        //         success: row.get("success"),
        //         // stdout: task.get("stdout"),
        //         // stderr: task.get("stderr"),
        //         // template_args_str: task.get("template_args_str"),
        //         resolved_args_str: row.get("resolved_args_str"),
        //         started: row.get("started"),
        //         ended: row.get("ended"),
        //         elapsed: row.get::<i32, _>("elapsed") as i64,
        //         premature_failure: row.get("premature_failure"),
        //         premature_failure_error_str: row.get("premature_failure_error_str"),
        //         is_branch: row.get("is_branch"),
        //     });
        // }
        // results
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn get_runs(dag_name: &str, pool: Pool) -> Vec<Run> {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //

        // let rows =
        //     sqlx::query("SELECT run_id, logical_date, timestamp FROM runs WHERE dag_name = $1")
        //         .bind(dag_name)
        //         .fetch_all(&pool)
        //         .await
        //         .unwrap();

        // let mut v = vec![];
        // for row in rows {
        //     // let mut value = json!({});

        //     let run_id: i32 = row.get(0);
        //     // value["run_id"] = run_id.to_string().into();

        //     let date: DateTime<Utc> = row.get(1);
        //     // value["date"] = date.to_string().into();

        //     v.push(Run {
        //         run_id: run_id as usize,
        //         date,
        //     });
        // }

        // v

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
        //     })
        // })
    }

    // pub async fn get_incomplete_tasks() {
    //     tokio::task::block_in_place(|| {
    //         tokio::runtime::Handle::current().block_on(async {
    //             //

    //             // Db::get_runs(dag_name)
    //         })
    //     })
    // }
    // #[timed(duration(printer = "debug!"))]
    // pub async fn get_pending_runs(dag_name: &str, pool: Pool) -> Vec<(usize, String)> {
    //     // tokio::task::block_in_place(|| {
    //     //     tokio::runtime::Handle::current().block_on(async {
    //     // TODO get only incomplete
    //     // let rows =
    //     //     sqlx::query("SELECT run_id, dag_id FROM runs WHERE dag_name = $1 AND status = $2")
    //     //         .bind(dag_name)
    //     //         .bind("Pending")
    //     //         .fetch_all(&pool)
    //     //         .await
    //     //         .unwrap();

    //     // let mut downstream_tasks = Vec::new();

    //     // for row in rows {
    //     //     let downstream_task_id: i32 = row.get(0);
    //     //     let downstream_task_id1: String = row.get(1);
    //     //     downstream_tasks.push((downstream_task_id as usize, downstream_task_id1));
    //     // }

    //     // downstream_tasks
    //     // Db::get_runs(dag_name)
    //     //     })
    //     // })
    //     todo!()
    // }

    // #[timed(duration(printer = "debug!"))]
    // pub async fn init_tables(pool: Pool) {
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE runs (
    //     //         run_id  SERIAL,
    //     //         dag_id  TEXT,
    //     //         dag_name    TEXT,
    //     //         timestamp timestamptz not null default NOW(),
    //     //         logical_date timestamptz not null,
    //     //         status TEXT NOT NULL DEFAULT 'Pending',
    //     //         PRIMARY KEY (run_id, dag_id)
    //     //     );
    //     //     ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE tasks (
    //     //         run_id  INT,
    //     //         task_id INT,
    //     //         function_name   TEXT NOT NULL,
    //     //         template_args   JSONB,
    //     //         options         JSONB NOT NULL,
    //     //         lazy_expand     BOOL NOT NULL,
    //     //         is_dynamic      BOOL NOT NULL,
    //     //         is_branch       BOOL NOT NULL,
    //     //         PRIMARY KEY (run_id, task_id)
    //     //     );
    //     //     ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE task_results (
    //     //         run_id      INT NOT NULL,
    //     //         task_id     INT NOT NULL,
    //     //         result      JSONB NOT NULL,
    //     //         attempt     INT NOT NULL,
    //     //         max_attempts    INT NOT NULL,
    //     //         function_name   TEXT NOT NULL,
    //     //         success         BOOL NOT NULL,
    //     //         resolved_args_str   TEXT NOT NULL,
    //     //         started             TEXT NOT NULL,
    //     //         ended               TEXT NOT NULL,
    //     //         elapsed             INT NOT NULL,
    //     //         premature_failure   BOOL NOT NULL,
    //     //         premature_failure_error_str TEXT NOT NULL,
    //     //         is_branch         BOOL NOT NULL,
    //     //         timestamp timestamptz not null default NOW()
    //     //     );
    //     //     ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE task_statuses (
    //     //         run_id      INT NOT NULL,
    //     //         task_id     INT NOT NULL,
    //     //         status      TEXT NOT NULL,
    //     //         timestamp timestamptz not null default NOW()
    //     //     );
    //     //     ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE edges (
    //     //         run_id  INT,
    //     //         up      INT,
    //     //         down    INT,
    //     //         PRIMARY KEY (run_id, up, down)
    //     //     );
    //     //     ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE dep_keys (
    //     //         run_id INT,
    //     //         task_id INT,
    //     //         upstream_task_id INT,
    //     //         template_arg_key TEXT,
    //     //         upstream_result_key TEXT
    //     //     );
    //     // ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();

    //     // sqlx::query(
    //     //     "
    //     //     CREATE TABLE logs (
    //     //         run_id INT,
    //     //         task_id INT,
    //     //         attempt INT,
    //     //         log TEXT
    //     //     );
    //     // ",
    //     // )
    //     // .execute(&pool)
    //     // .await
    //     // .ok();
    // }

    #[timed(duration(printer = "debug!"))]
    pub async fn contains_logical_date(
        dag_name: &str,
        dag_hash: &str,
        logical_date: DateTime<Utc>,
        pool: Pool,
    ) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                //
                let mut conn = pool.get().await.unwrap();
                cmd("SISMEMBER")
                    .arg(&[
                        format!("{LOGICAL_DATES_KEY}:{dag_name}:{dag_hash}"),
                        logical_date.to_string(),
                    ])
                    .query_async::<_, bool>(&mut conn)
                    .await
                    .unwrap();
                // let task = sqlx::query(
                //             "SELECT EXISTS(SELECT 1 FROM runs WHERE dag_id = $1 AND dag_name = $2 AND logical_date = $3 )",
                //         )
                //         .bind(dag_hash)
                //         .bind(dag_name)
                //         .bind(logical_date)
                //         .fetch_one(&pool)
                //         .await;
                // task.unwrap().get(0)
                todo!();
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
    fn get_log(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        attempt: usize,
        // pool: Pool,
    ) -> String {
        // let pool = self.redis
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                //         //
                //         let task = sqlx::query(
                //             "SELECT log FROM logs WHERE run_id = $1 AND task_id = $2 AND attempt = $3",
                //         )
                //         .bind(*dag_run_id as i32)
                //         .bind(*task_id as i32)
                //         .bind(attempt as i32)
                //         .fetch_one(&self.pool)
                //         .await;
                //         task.unwrap().get(0)

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

    // fn init_log(&mut self, dag_run_id: &usize, task_id: &usize, attempt: usize) {
    //     // let task_logs = self.task_logs.clone();
    //     let task_id = *task_id;

    //     // let mut task_logs = task_logs.lock().unwrap();
    //     // if !task_logs.contains_key(&task_id) {
    //     //     task_logs.insert(task_id, s);
    //     // } else {
    //     //     *task_logs.get_mut(&task_id).unwrap() += &s;
    //     // }
    //     tokio::task::block_in_place(|| {
    //         tokio::runtime::Handle::current().block_on(async {
    //             sqlx::query(
    //                 "INSERT INTO logs (run_id, task_id, attempt, log)
    //                 VALUES ($1, $2, $3, $4);",
    //             )
    //             .bind(*dag_run_id as i32)
    //             .bind(task_id as i32)
    //             .bind(attempt as i32)
    //             .bind("")
    //             // .bind(v)
    //             .execute(&self.pool)
    //             .await
    //             .unwrap();
    //         })
    //     });
    // }
    #[timed(duration(printer = "debug!"))]
    fn handle_log(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        attempt: usize,
    ) -> Box<dyn Fn(String) + Send> {
        // let task_logs = self.task_logs.clone();
        let task_id = *task_id;
        let dag_run_id = *dag_run_id;
        let pool = self.pool.clone();
        // let log = self.get_log(&dag_run_id, &task_id, attempt);

        Box::new(move |s| {
            // let mut task_logs = task_logs.lock().unwrap();
            // if !task_logs.contains_key(&task_id) {
            //     task_logs.insert(task_id, s);
            // } else {
            //     *task_logs.get_mut(&task_id).unwrap() += &s;
            // }
            // let redis = get_redis_client();
            // tokio::task::block_in_place(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                // sqlx::query(
                //     "UPDATE logs
                //     SET log = log || $4
                //     WHERE run_id = $1 AND task_id = $2 AND attempt = $3",
                // )
                // .bind(dag_run_id as i32)
                // .bind(task_id as i32)
                // .bind(attempt as i32)
                // .bind(s)
                // .execute(&pool)
                // .await
                // .unwrap();

                let mut conn = pool.get().await.unwrap();
                cmd("RPUSH")
                    .arg(&dbg!(format!("{LOG_KEY}:{dag_run_id}:{task_id}:{attempt}")))
                    .arg(s)
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
                // .iter()
                // .map(|v| serde_json::from_str(v).unwrap())
                // .collect()
            });
            // });
        })
    }
    // //
    // #[timed(duration(printer = "debug!"))]
    // fn is_task_completed(&self, dag_run_id: &usize, task_id: &usize) -> bool {
    //     match  self.get_task_status(dag_run_id, task_id) {
    //         TaskStatus::Pending | TaskStatus::Running | TaskStatus::Retrying => false,
    //         TaskStatus::Success | TaskStatus::Failure | TaskStatus::Skipped => true,
    //     }
    //     // if self.get_task_status(dag_run_id, task_id) == TaskStatus::Skipped {
    //     //     return true;
    //     // }

    //     // let mut redis = Db::get_redis_client();
    //     // if redis
    //     //     .exists(format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"))
    //     //     .unwrap()
    //     // {
    //     //     return !self.get_task_result(dag_run_id, task_id).needs_retry();
    //     // }
    //     // false
    // }

    #[timed(duration(printer = "debug!"))]
    fn get_dag_name(&self) -> String {
        self.name.clone()
    }

    // #[timed(duration(printer = "debug!"))]
    // fn set_status_to_running_if_possible(&mut self, dag_run_id: &usize, task_id: &usize) -> bool {
    //     tokio::task::block_in_place(|| {
    //         tokio::runtime::Handle::current().block_on(async {
    //             // let mut redis = Db::get_redis_client();
    //             // let _: Result<(), redis::RedisError> = self
    //             //     .redis
    //             //     .set(format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}"), "Running");

    //             // // Update the task status to "Running" if its last status is "Pending" or "Retrying"
    //             // let rows_affected = sqlx::query(
    //             //     "
    //             //     UPDATE task_statuses
    //             //     SET status = 'Running'
    //             //     WHERE run_id = $1 AND task_id = $2 AND (
    //             //         SELECT status
    //             //         FROM task_statuses
    //             //         WHERE run_id = $1 AND task_id = $2
    //             //         ORDER BY timestamp DESC
    //             //         LIMIT 1
    //             //     ) IN ('Pending', 'Retrying');

    //             // ",
    //             // )
    //             // .bind(*dag_run_id as i32)
    //             // .bind(*task_id as i32)
    //             // .execute(&self.pool)
    //             // .await
    //             // .unwrap()
    //             // .rows_affected();

    //             // rows_affected > 0
    //             todo!()
    //         })
    //     })
    // }

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
                // .iter()
                // .map(|v| v).unwrap())
                // .collect()
            })
        })
        // let mut redis = Db::get_redis_client();
        // let result: Result<String, redis::RedisError> = self
        //     .redis
        //     .get(format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"));

        // if result.is_ok() {
        //     println!("hit cache - {TASK_RESULT_KEY}:{dag_run_id}:{task_id}");
        //     serde_json::from_str(&result.unwrap()).unwrap()
        // } else {
        //     println!("cache missed - {TASK_RESULT_KEY}:{dag_run_id}:{task_id}");

        //     let res = tokio::task::block_in_place(|| {
        //         tokio::runtime::Handle::current().block_on(async {
        //             let task = sqlx::query(
        //                 "
        //             SELECT *
        //             FROM task_results
        //             WHERE run_id = $1 AND task_id = $2
        //             ORDER BY timestamp DESC
        //             LIMIT 1;
        //             ",
        //             )
        //             .bind(*dag_run_id as i32)
        //             .bind(*task_id as i32)
        //             .fetch_one(&self.pool)
        //             .await
        //             .unwrap();
        //             // let id: i32 = task.get("task_id");
        //             // let name: String = task.get("name");
        //             // let function_name: String = task.get("function_name");
        //             // let template_args: Value =
        //             //     serde_json::from_value(task.get("template_args")).unwrap();
        //             // let options: TaskOptions = serde_json::from_value(task.get("options")).unwrap();
        //             // let lazy_expand: bool = task.get("lazy_expand");
        //             // let is_dynamic: bool = task.get("is_dynamic");
        //             TaskResult {
        //                 task_id: task.get::<i32, _>("task_id") as usize,
        //                 result: serde_json::from_value(task.get("result")).unwrap(),
        //                 // task_name: task.get("task_name"),
        //                 attempt: task.get::<i32, _>("attempt") as usize,
        //                 max_attempts: task.get::<i32, _>("max_attempts") as isize,
        //                 function_name: task.get("function_name"),
        //                 success: task.get("success"),
        //                 // stdout: task.get("stdout"),
        //                 // stderr: task.get("stderr"),
        //                 // template_args_str: task.get("template_args_str"),
        //                 resolved_args_str: task.get("resolved_args_str"),
        //                 started: task.get("started"),
        //                 ended: task.get("ended"),
        //                 elapsed: task.get::<i32, _>("elapsed") as i64,
        //                 premature_failure: task.get("premature_failure"),
        //                 premature_failure_error_str: task.get("premature_failure_error_str"),
        //                 is_branch: task.get("is_branch"),
        //             }
        //             // serde_json::from_value(v).unwrap()
        //         })
        //     });

        //     let _: Result<(), redis::RedisError> = self.redis.set(
        //         format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"),
        //         serde_json::to_string(&res).unwrap(),
        //     );

        //     res
        // }
    }

    #[timed(duration(printer = "debug!"))]
    fn get_attempt_by_task_id(&self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Count the number of results for the given dag_run_id and task_id
                // let result: i64 = sqlx::query(
                //     "SELECT COUNT(*) FROM task_results WHERE run_id = $1 AND task_id = $2",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(*task_id as i32)
                // .fetch_one(&self.pool)
                // .await
                // .unwrap()
                // .get(0);
                let mut conn = self.pool.get().await.unwrap();

                // result as usize + 1
                cmd("INCR")
                    .arg(&[format!("{TASK_ATTEMPT_KEY}:{dag_run_id}:{task_id}")])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()

                // todo!()
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
                // .iter()
                // .map(|v| v).unwrap())
                // .collect()
            })
        })
        // let mut status = TaskStatus::Pending.as_str().to_string();

        // // let mut redis = Db::get_redis_client();
        // let result = self
        //     .redis
        //     .get(format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}"));

        // let task_status = if result.is_ok() {
        //     println!("hit cache - {TASK_STATUS_KEY}:{dag_run_id}:{task_id}");
        //     result.unwrap()
        // } else {
        //     println!("cache missed - {TASK_STATUS_KEY}:{dag_run_id}:{task_id}");

        //     result.unwrap_or_else(|_| {
        //         tokio::task::block_in_place(|| {
        //             tokio::runtime::Handle::current().block_on(async {
        //                 // SORT
        //                 let s = sqlx::query(
        //                     "
        //                             SELECT status
        //                             FROM task_statuses
        //                             WHERE run_id = $1 AND task_id = $2
        //                             ORDER BY timestamp DESC
        //                             LIMIT 1;
        //                         ",
        //                 )
        //                 .bind(*dag_run_id as i32)
        //                 .bind(*task_id as i32)
        //                 .fetch_optional(&self.pool)
        //                 .await
        //                 .unwrap();

        //                 // Convert the string status to TaskStatus enum (assuming you have a function or method for that)
        //                 if let Some(s) = s {
        //                     status = s.get(0);
        //                 }
        //             })
        //         });
        //         let _: Result<(), redis::RedisError> = self
        //             .redis
        //             .set(format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}"), &status);

        //         status.to_string()
        //     })
        // };

        // TaskStatus::from_str(&task_status).unwrap()
    }

    // fn get_pending_run_ids(&self) -> Vec<String> {
    //     todo!()
    // }

    // fn get_all_statuses(&self, dag_run_id: &usize) -> Vec<(String, TaskStatus)> {
    //     todo!()
    // }

    // fn insert_attempt(&mut self, dag_run_id: &usize, result: &TaskResult) {
    //     todo!()
    // }

    // fn insert_task_results(&mut self, dag_run_id: &usize, result: &TaskResult) {
    //     todo!()
    // }

    #[timed(duration(printer = "debug!"))]
    fn set_task_status(&mut self, dag_run_id: &usize, task_id: &usize, task_status: TaskStatus) {
        // let mut redis = Db::get_redis_client();
        // let _: Result<(), redis::RedisError> = self.redis.set(
        //     format!("{TASK_STATUS_KEY}:{dag_run_id}:{task_id}"),
        //     task_status.as_str(),
        // );

        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //         sqlx::query(
        //             "
        //             INSERT INTO task_statuses (run_id, task_id, status)
        //             VALUES ($1, $2, $3);
        //             ",
        //         )
        //         .bind(*dag_run_id as i32)
        //         .bind(*task_id as i32)
        //         .bind(task_status.as_str())
        //         .execute(&self.pool)
        //         .await
        //         .unwrap(); // Be cautious with unwrap, consider proper error handling
        //     })
        // });

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
                // .iter()
                // .map(|v| v).unwrap())
                // .collect()
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
                // sqlx::query(
                //     "
                //         INSERT INTO dags (dag_id, dag_name)
                //         VALUES ($1, $2)
                //         ON CONFLICT (dag_id, dag_name)
                //         DO NOTHING;
                //     ",
                // )
                // .bind(dag_hash)
                // .bind(dag_name)
                // .execute(&self.pool)
                // .await
                // .unwrap();

                // let dag_run_id: i32 = sqlx::query(
                //     "
                //         INSERT INTO runs (dag_id, dag_name, logical_date)
                //         VALUES ($1, $2, $3)
                //         RETURNING run_id;
                //     ",
                // )
                // .bind(dag_hash)
                // .bind(dag_name)
                // .bind(logical_date)
                // .fetch_one(&self.pool)
                // .await
                // .unwrap()
                // .get(0);
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
                // todo!();

                // .iter()
                // .map(|v| v).unwrap())
                // .collect()

                // for task in self.get_default_tasks() {
                //     // sqlx::query(
                //     //     "
                //     //     INSERT INTO tasks (run_id, task_id, name, function_name, template_args, options, lazy_expand, is_dynamic)
                //     //     VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
                //     //     ",
                //     // )
                //     // .bind(dag_run_id)
                //     // .bind(task.id as i64)
                //     // .bind(task.function_name)
                //     // .bind(task.template_args)
                //     // .bind(serde_json::to_value(task.options).unwrap())
                //     // .bind(task.lazy_expand)
                //     // .bind(task.is_dynamic)
                //     // .execute(&self.pool).await.unwrap();
                //     self.append_new_task(
                //         &(dag_run_id as usize),
                //         task.name,
                //         task.function_name,
                //         task.template_args,
                //         task.options,
                //         task.lazy_expand,
                //         task.is_dynamic,
                //     );
                // }

                // dag_run_id as usize
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_task_results(&mut self, dag_run_id: &usize, result: &TaskResult) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // sqlx::query(
                //     "
                //     INSERT INTO task_results (
                //         run_id, task_id, result, attempt, max_attempts,
                //         function_name, success, resolved_args_str, started, ended, elapsed,
                //         premature_failure, premature_failure_error_str, is_branch
                //     )
                //     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(result.task_id as i32)
                // .bind(&result.result)
                // // .bind(&result.task_name)
                // .bind(result.attempt as i32)
                // .bind(result.max_attempts as i32)
                // .bind(&result.function_name)
                // .bind(result.success)
                // // .bind(&result.stdout)
                // // .bind(&result.stderr)
                // // .bind(&result.template_args_str)
                // .bind(&result.resolved_args_str)
                // .bind(&result.started)
                // .bind(&result.ended)
                // .bind(result.elapsed)
                // .bind(result.premature_failure)
                // .bind(&result.premature_failure_error_str)
                // .bind(result.is_branch)
                // .execute(&self.pool)
                // .await
                // .unwrap();

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
                // let mut conn = self.redis.get().await.unwrap();
                cmd("SET")
                    .arg(&[format!("{TASK_RESULT_KEY}:{dag_run_id}:{task_id}"), res])
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .unwrap();
            })
        });

        // let mut redis = Db::get_redis_client();
        // let _: Result<(), redis::RedisError> = self.redis.set(
        //     format!("{TASK_RESULT_KEY}:{dag_run_id}:{}", result.task_id),
        //     serde_json::to_string(result).unwrap(),
        // );
    }

    // #[timed(duration(printer = "debug!"))]
    // fn mark_finished(&self, dag_run_id: &usize) {
    //     dbg!("mark finished!");
    //     tokio::task::block_in_place(|| {
    //         tokio::runtime::Handle::current().block_on(async {
    //             // sqlx::query("UPDATE runs SET status = $2 WHERE run_id = $1")
    //             //     .bind(*dag_run_id as i32)
    //             //     .bind("Completed")
    //             //     // .bind(v)
    //             //     .execute(&self.pool)
    //             //     .await
    //             //     .unwrap();
    //             // todo!()
    //         })
    //     });
    // }

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
        // let mut results: HashMap<(usize, Option<String>), Option<String>> = HashMap::new();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // let rows = sqlx::query(
                //     "
                //         SELECT upstream_task_id, template_arg_key, upstream_result_key
                //         FROM dep_keys
                //         WHERE run_id = $1 AND task_id = $2;
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(*task_id as i32)
                // .fetch_all(&self.pool)
                // .await
                // .unwrap();

                // for row in rows {
                //     let upstream_task_id: i32 = row.get(0);
                //     let template_arg_key = row.get(1);
                //     let upstream_result_key: Option<String> = row.get(2);

                //     results.insert(
                //         (upstream_task_id as usize, template_arg_key),
                //         upstream_result_key,
                //     );
                // }
                let mut conn = self.pool.get().await.unwrap();

                let k: Vec<((usize, String), String)> = cmd("SMEMBERS")
                    .arg(&dbg!(format!("{DEPENDENCY_KEYS_KEY}:{dag_run_id}:{task_id}")))
                    // .arg(0)
                    // .arg(-1)
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|v| serde_json::from_str(v).unwrap())
                    .collect();

                // let m: Vec<((usize, String), String)> = serde_json::from_str(&k).unwrap();
                // let mut h = HashMap::new();

                // for j in k {
                //     h.insert(j.0, j.1);
                // }
                k.into_iter().collect()
                // todo!()
            })
        })

        // results
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
                // sqlx::query(
                //     "
                //             INSERT INTO dep_keys (run_id, task_id, upstream_task_id, template_arg_key, upstream_result_key)
                //             VALUES ($1, $2, $3, $4, $5);
                //         ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(*task_id as i32)
                // .bind(upstream.0 as i32)
                // .bind(upstream.1)
                // .bind(v)
                // .execute(&self.pool)
                // .await
                // .unwrap();
                // todo!()

                // let (upstream_task_id, template_arg_key) = upstream;
                // let template_arg_key = template_arg_key.unwrap_or("".into());

                // let mut dep_keys = self.get_dependency_keys(dag_run_id, task_id);
                // dep_keys.insert(upstream, v);

                let mut conn = self.pool.get().await.unwrap();
                // let mut vals = vec![];

                // for (k, v) in dep_keys.drain() {
                //     vals.push((k, v));
                // }
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
                // let rows = sqlx::query("SELECT down FROM edges WHERE run_id = $1 AND up = $2")
                //     .bind(*dag_run_id as i32)
                //     .bind(*task_id as i32)
                //     .fetch_all(&self.pool)
                //     .await
                //     .unwrap();

                // let mut downstream_tasks = HashSet::new();

                // for row in rows {
                //     let downstream_task_id: i32 = row.get(0);
                //     downstream_tasks.insert(downstream_task_id as usize);
                // }

                // downstream_tasks

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
                // let rows = sqlx::query("SELECT up FROM edges WHERE run_id = $1 AND down = $2")
                //     .bind(*dag_run_id as i32)
                //     .bind(*task_id as i32)
                //     .fetch_all(&self.pool)
                //     .await
                //     .unwrap();

                // let mut upstream_tasks = HashSet::new();

                // for row in rows {
                //     let upstream_task_id: i32 = row.get(0);
                //     upstream_tasks.insert(upstream_task_id as usize);
                // }

                // upstream_tasks

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
                // sqlx::query(
                //     "
                //         DELETE FROM edges
                //         WHERE run_id = $1 AND up = $2 AND down = $3;
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(edge.0 as i32)
                // .bind(edge.1 as i32)
                // .execute(&self.pool)
                // .await
                // .unwrap();

                // sqlx::query(
                //     "
                //         DELETE FROM dep_keys
                //         WHERE run_id = $1 AND task_id = $2 AND upstream_task_id = $3 AND template_arg_key IS NULL;
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(edge.1 as i32)
                // .bind(edge.0 as i32)
                // .execute(&self.pool)
                // .await
                // .unwrap();

                let mut conn = self.pool.get().await.unwrap();
                cmd("SREM")
                    .arg(&[
                        format!("{EDGES_KEY}:{dag_run_id}"),
                        serde_json::to_string(edge).unwrap(),
                    ])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap();

                // let mut dep_keys = self.get_dependency_keys(dag_run_id, &edge.1);
                // dep_keys.retain(|k, _| k != &(edge.0, "".into()));

                // // self.set_dependency_keys(dag_run_id, task_id, upstream, v)

                // let mut vals = vec![];

                // for (k, v) in dep_keys.drain() {
                //     vals.push((k, v));
                // }
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
                // sqlx::query(
                //     "
                //             INSERT INTO edges (run_id, up, down)
                //             VALUES ($1, $2, $3)
                //             ON CONFLICT (run_id, up, down)
                //             DO NOTHING;
                //         ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(edge.0 as i32)
                // .bind(edge.1 as i32)
                // .execute(&self.pool)
                // .await
                // .unwrap();

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
    fn get_all_tasks_incomplete(&mut self, dag_run_id: &usize) -> Vec<Task> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // TODO sort?
                // let tasks = sqlx::query("SELECT * FROM tasks WHERE run_id = $1")
                //     .bind(*dag_run_id as i32)
                //     .fetch_all(&self.pool)
                //     .await
                //     .unwrap();

                self.get_all_tasks(dag_run_id)
                    .iter()
                    .filter_map(|task| {
                        if self.is_task_completed(dag_run_id, &task.id) {
                            return None;
                        }
                        Some(task.to_owned())
                    })
                    .collect()

                // tasks
                //     .iter()
                //     // .filter(|t| t.get)
                //     .filter_map(|task| {
                //         let id: i32 = task.get("task_id");

                //         let function_name: String = task.get("function_name");
                //         let template_args: Value =
                //             serde_json::from_value(task.get("template_args")).unwrap();
                //         let options: TaskOptions =
                //             serde_json::from_value(task.get("options")).unwrap();
                //         let lazy_expand: bool = task.get("lazy_expand");
                //         let is_dynamic: bool = task.get("is_dynamic");
                //         let is_branch: bool = task.get("is_branch");
                //         Some(Task {
                //             id: id as usize,
                //             function_name,
                //             template_args,
                //             options,
                //             lazy_expand,
                //             is_dynamic,
                //             is_branch,
                //         })
                //     })
                //     // .filter(|t| )
                //     .collect()

                // todo!()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_all_tasks(&self, dag_run_id: &usize) -> Vec<Task> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // TODO sort?
                // let tasks = sqlx::query("SELECT * FROM tasks WHERE run_id = $1")
                //     .bind(*dag_run_id as i32)
                //     .fetch_all(&self.pool)
                //     .await
                //     .unwrap();

                // tasks
                //     .iter()
                //     .map(|task| {
                //         let id: i32 = task.get("task_id");
                //         let function_name: String = task.get("function_name");
                //         let template_args: Value =
                //             serde_json::from_value(task.get("template_args")).unwrap();
                //         let options: TaskOptions =
                //             serde_json::from_value(task.get("options")).unwrap();
                //         let lazy_expand: bool = task.get("lazy_expand");
                //         let is_dynamic: bool = task.get("is_dynamic");
                //         let is_branch: bool = task.get("is_branch");
                //         Task {
                //             id: id as usize,
                //             function_name,
                //             template_args,
                //             options,
                //             lazy_expand,
                //             is_dynamic,
                //             is_branch,
                //         }
                //     })
                //     .collect()

                let mut conn: deadpool_redis::Connection = self.pool.get().await.unwrap();
                cmd("SMEMBERS")
                    .arg(&format!("{TASKS_KEY}:{dag_run_id}"))
                    // .arg(0)
                    // .arg(-1)
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
                // let task =
                //     sqlx::query("SELECT * FROM tasks WHERE run_id = $1 AND task_id = $2 LIMIT 1")
                //         .bind(*dag_run_id as i32)
                //         .bind(*task_id as i32)
                //         .fetch_one(&self.pool)
                //         .await
                //         .unwrap();
                // let id: i32 = task.get("task_id");
                // // let name: String = task.get("name");
                // let function_name: String = task.get("function_name");
                // let template_args: Value =
                //     serde_json::from_value(task.get("template_args")).unwrap();
                // let options: TaskOptions = serde_json::from_value(task.get("options")).unwrap();
                // let lazy_expand: bool = task.get("lazy_expand");
                // let is_dynamic: bool = task.get("is_dynamic");
                // let is_branch: bool = task.get("is_branch");

                // Task {
                //     id: id as usize,
                //     function_name,
                //     template_args,
                //     options,
                //     lazy_expand,
                //     is_dynamic,
                //     is_branch,
                // }

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
                // let q: i32 = sqlx::query(
                //     "
                //         INSERT INTO tasks (run_id, task_id, function_name, template_args, options, lazy_expand, is_dynamic, is_branch)
                //         VALUES (
                //             $1,
                //             (SELECT COALESCE(MAX(task_id) + 1, 0) FROM tasks WHERE run_id = $1),
                //             $2, $3, $4, $5, $6, $7
                //         )
                //         RETURNING task_id;
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(function_name)
                // .bind(template_args)
                // .bind(serde_json::to_value(options).unwrap())
                // .bind(lazy_expand)
                // .bind(is_dynamic)
                // .bind(is_branch)
                // .fetch_one(&self.pool).await.unwrap().get(0);
                let mut conn = self.pool.get().await.unwrap();

                let task_id = cmd("INCR")
                    .arg(&[format!("{TASK_ID_KEY}:{dag_run_id}")])
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
                    - 1;

                // self.set_task_status(dag_run_id, &(q as usize), TaskStatus::Pending);
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
                // q as usize
                task_id
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_template_args(&self, dag_run_id: &usize, task_id: &usize) -> serde_json::Value {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // let k = sqlx::query(
                //     "
                //     SELECT template_args
                //     FROM tasks
                //     WHERE run_id = $1 AND task_id = $2
                //     LIMIT 1;
                //     ",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(*task_id as i32)
                // .fetch_optional(&self.pool)
                // .await;

                // // if k.is_err() {
                // //     // dbg!(&dag_run_id, &task_id);
                // //     panic!();
                // // }

                // k.unwrap().unwrap().get(0)

                // let mut conn = self.redis.get().await.unwrap();

                // serde_json::from_str(
                //     &cmd("GET")
                //         .arg(&[dbg!(format!("{TEMPLATE_ARGS_KEY}:{dag_run_id}:{task_id}"))])
                //         .query_async::<_, String>(&mut conn)
                //         .await
                //         .unwrap(),
                // )
                // .unwrap()
                let task = self.get_task_by_id(dag_run_id, task_id);
                task.template_args
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_template_args(&mut self, dag_run_id: &usize, task_id: &usize, template_args_str: &str) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // let v: Value = serde_json::from_str(template_args_str).unwrap();

                // sqlx::query(
                //     "UPDATE tasks SET template_args = $3 WHERE run_id = $1 AND task_id = $2",
                // )
                // .bind(*dag_run_id as i32)
                // .bind(*task_id as i32)
                // .bind(v)
                // .execute(&self.pool)
                // .await
                // .unwrap();

                let mut conn = self.pool.get().await.unwrap();
                let mut task = self.get_task_by_id(dag_run_id, task_id);
                task.template_args = serde_json::from_str(template_args_str).unwrap();

                dbg!(&task.template_args);

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

    // fn get_priority_queue(&mut self) -> Vec<QueuedTask> {
    //     todo!()
    // }
    #[timed(duration(printer = "debug!"))]
    fn pop_priority_queue(&mut self) -> Option<OrderedQueuedTask> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // transaction_async!(&mut self.redis.get().await.unwrap(), &["queue_watch"], {
                let mut conn = self.pool.get().await.unwrap();

                let res = cmd("ZPOPMIN")
                    .arg(&["queue".to_string(), "1".to_string()]) // TODO timeout arg
                    .query_async::<_, Vec<String>>(&mut conn)
                    .await;

                // conn.

                // let mut pipe = redis::pipe();
                // let pipe_ref = pipe.atomic();
                // pipe_ref.zpopmin(format!("queue"), 1);
                // let res = pipe.query_async::<_, Value>(&mut conn).await;

                // // Begin the transaction
                // let mut pipe = redis::pipe();
                // pipe.atomic()
                //     .cmd("SET")
                //     .arg("key1")
                //     .arg("value1")
                //     .cmd("SET")
                //     .arg("key2")
                //     .arg("value2");

                // Execute the transaction
                // let result: (String, String) = pipe.query_async(&mut conn).await.unwrap();
                // println!("Transaction result: {:?}", result);

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
                // })
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn push_priority_queue(&mut self, queued_task: OrderedQueuedTask) {
        // let depth = self.get_task_depth(dag_run_id, &queued_task.task_id);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // transaction_async!(&mut self.redis.get().await.unwrap(), &["queue_watch"], {
                let mut conn = self.pool.get().await.unwrap();

                // let mut pipe = redis::pipe();
                // let pipe_ref = pipe.atomic();
                // pipe_ref.zadd(
                //     format!("queue"),
                //     serde_json::to_string(&queued_task).unwrap(),
                //     queued_task.depth,
                // );
                cmd("ZADD")
                    .arg(&format!("queue"))
                    .arg(dbg!(queued_task.score))
                    .arg(dbg!(serde_json::to_string(&queued_task.task).unwrap()))
                    .query_async::<_, f64>(&mut conn)
                    .await
                    .unwrap();
                // pipe_ref
                //     // .del(keys_to_del)
                //     // .srem(&self.redis_manager.queues_key, &self.name)
                //     .query_async::<_, ()>(&mut conn)
                //     .await
                //     .unwrap()

                // conn.

                // // Begin the transaction
                // let mut pipe = redis::pipe();
                // pipe.atomic()
                //     .cmd("SET")
                //     .arg("key1")
                //     .arg("value1")
                //     .cmd("SET")
                //     .arg("key2")
                //     .arg("value2");

                // Execute the transaction
                // let result: (String, String) = pipe.query_async(&mut conn).await.unwrap();
                // println!("Transaction result: {:?}", result);

                // if let Some(res) = res {
                //     cmd("ZADD")
                //         .arg(&[format!("tmpqueue"), res.to_string()])
                //         .query_async::<_, ()>(&mut conn)
                //         .await
                //         .unwrap();
                //     return Some(serde_json::from_str(&res).unwrap());
                // }

                // None
                // return None
                // });
            });
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn priority_queue_len(&self) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                cmd("ZCARD")
                    .arg("queue")
                    .query_async::<_, usize>(&mut conn)
                    .await
                    .unwrap()
            })
        })
    }

    fn get_task_depth(&mut self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut conn = self.pool.get().await.unwrap();

                // cmd("GET")
                //     .arg(&format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"))
                //     .query_async::<_, usize>(&mut conn)
                //     .await
                //     .unwrap()
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
                    .arg(&[format!("{DEPTH_KEY}:{dag_run_id}:{task_id}"), depth.to_string()])
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
    fn print_priority_queue(&mut self) {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {  })
        // });
    }
}

// struct DepKeys {
//     pub inner: HashMap<(usize, Option<String>), Option<String>>,
// }

// impl Serialize for DepKeys {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&format!("{}:{}", self.rise, self.distance))
//     }
// }
