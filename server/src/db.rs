use log::{info,debug};
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::{Commands, Connection, JsonCommands, RedisResult};
use thepipelinetool::prelude::*;
// use runner::Runner;
// use rocket::tokio;
// use backend::BackendTrait;
use log::LevelFilter;
use serde_json::Value;

// use task::task_options::TaskOptions;
// use task::{task::Task, task_result::TaskResult, task_status::TaskStatus};

#[derive(Clone)]
pub struct Db {
    edges: HashSet<(usize, usize)>,
    nodes: Vec<Task>,
    name: String,
}

// impl Default for Postgres {
//     fn default() -> Self {
//         Self {}
//     }
// }

use sqlx::{ConnectOptions, Pool, Postgres, Row};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use timed::timed;
// use task::task::Task;
// use task::task_options::TaskOptions;
// use task::task_result::TaskResult;
// use task::task_status::TaskStatus;
// use thepipelinetool::prelude::*;

fn get_db_url() -> String {
    env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:example@0.0.0.0:5432".to_string())
        .to_string()
}

fn get_redis_url() -> String {
    env::var("REDIS_URL")
        .unwrap_or("redis://0.0.0.0:6379".to_string())
        .to_string()
}

impl Db {
    #[timed(duration(printer = "debug!"))]
    pub async fn get_runs(dag_name: &str) -> Vec<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let rows = sqlx::query("SELECT run_id FROM runs WHERE dag_name = $1")
                    .bind(dag_name)
                    .fetch_all(&client)
                    .await
                    .unwrap();

                let mut downstream_tasks = Vec::new();

                for row in rows {
                    let downstream_task_id: i32 = row.get(0);
                    downstream_tasks.push(downstream_task_id as usize);
                }

                downstream_tasks
            })
        })
    }

    // pub async fn get_incomplete_tasks() {
    //     tokio::task::block_in_place(|| {
    //         tokio::runtime::Handle::current().block_on(async {
    //             // let client = Db::get_client().await;

    //             // Db::get_runs(dag_name)
    //         })
    //     })
    // }
    #[timed(duration(printer = "debug!"))]
    pub async fn get_all_runs(dag_name: &str) -> Vec<(usize, String)> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                // TODO get only incomplete
                let rows = sqlx::query(
                    "SELECT run_id, dag_id FROM runs WHERE dag_name = $1 AND status = $2",
                )
                .bind(dag_name)
                .bind("Pending")
                .fetch_all(&client)
                .await
                .unwrap();

                let mut downstream_tasks = Vec::new();

                for row in rows {
                    let downstream_task_id: i32 = row.get(0);
                    let downstream_task_id1: String = row.get(1);
                    downstream_tasks.push((downstream_task_id as usize, downstream_task_id1));
                }

                downstream_tasks
                // Db::get_runs(dag_name)
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn init_tables() {
        let client = Db::get_client().await;
        // dbg!(1);

        // sqlx::query(
        //     "
        //     CREATE TABLE dags (
        //         dag_id      TEXT,
        //         dag_name    TEXT,
        //         PRIMARY KEY (dag_id, dag_name)
        //     );
        // ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        sqlx::query(
            "
            CREATE TABLE runs (
                run_id  SERIAL,
                dag_id  TEXT,
                dag_name    TEXT,
                timestamp timestamptz not null default NOW(),
                logical_date timestamptz not null,
                status TEXT NOT NULL DEFAULT 'Pending',
                PRIMARY KEY (run_id, dag_id)
            );
            ",
        )
        .execute(&client)
        .await
        .ok();
        sqlx::query(
            "
            CREATE TABLE tasks (
                run_id  INT,
                task_id INT,
                function_name   TEXT NOT NULL,
                template_args   JSONB,
                options         JSONB NOT NULL,
                lazy_expand     BOOL NOT NULL,
                is_dynamic      BOOL NOT NULL,
                is_branch       BOOL NOT NULL,
                PRIMARY KEY (run_id, task_id)
            );
            ",
        )
        .execute(&client)
        .await
        .ok();
        sqlx::query(
            "
            CREATE TABLE task_results (
                run_id      INT NOT NULL,
                task_id     INT NOT NULL,
                result      JSONB NOT NULL,
                attempt     INT NOT NULL,
                max_attempts    INT NOT NULL,
                function_name   TEXT NOT NULL,
                success         BOOL NOT NULL,
                stdout          TEXT NOT NULL,
                stderr          TEXT NOT NULL,
                template_args_str   TEXT NOT NULL,
                resolved_args_str   TEXT NOT NULL,
                started             TEXT NOT NULL,
                ended               TEXT NOT NULL,
                elapsed             INT NOT NULL,
                premature_failure   BOOL NOT NULL,
                premature_failure_error_str TEXT NOT NULL,
                is_branch         BOOL NOT NULL,
                timestamp timestamptz not null default NOW()
            );
            ",
        )
        .execute(&client)
        .await
        .ok();
        sqlx::query(
            "
            CREATE TABLE task_statuses (
                run_id      INT NOT NULL,
                task_id     INT NOT NULL,
                status      TEXT NOT NULL,
                timestamp timestamptz not null default NOW()
            );
            ",
        )
        .execute(&client)
        .await
        .ok();
        sqlx::query(
            "
            CREATE TABLE edges (
                run_id  INT,
                up      INT,
                down    INT,
                PRIMARY KEY (run_id, up, down)
            );
            ",
        )
        .execute(&client)
        .await
        .ok();
        sqlx::query(
            "
            CREATE TABLE dep_keys (
                run_id INT,
                task_id INT,
                upstream_task_id INT,
                template_arg_key TEXT,
                upstream_result_key TEXT
            );
        ",
        )
        .execute(&client)
        .await
        .ok();
    }

    #[timed(duration(printer = "debug!"))]
    pub fn get_redis_client() -> Connection {
        let client = redis::Client::open(get_redis_url()).unwrap();
        client.get_connection().unwrap()
    }

    #[timed(duration(printer = "debug!"))]
    async fn get_client() -> Pool<Postgres> {
        // let options = PgConnectOptions::from_str(&get_db_url())
        //     .unwrap()
        //     // .log_statements(LevelFilter::Debug);
        //     .log_slow_statements(LevelFilter::Debug, Duration::new(0, 500_000_000).clone());

        // PgPoolOptions::new().connect_with(options).await.unwrap()

        PgPoolOptions::new().connect(&get_db_url()).await.unwrap()

        // sqlx::query(
        //     "
        //         CREATE TABLE dags (
        //             dag_id      TEXT PRIMARY KEY
        //         );
        //     ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE runs (
        //             run_id  SERIAL,
        //             dag_id  TEXT,
        //             timestamp timestamptz not null default NOW(),
        //             PRIMARY KEY (run_id, dag_id)
        //         );
        //         ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE tasks (
        //             run_id  INT,
        //             task_id INT,
        //             name    TEXT NOT NULL,
        //             function_name   TEXT NOT NULL,
        //             template_args   JSONB,
        //             options         JSONB NOT NULL,
        //             lazy_expand     BOOL NOT NULL,
        //             is_dynamic      BOOL NOT NULL,
        //             PRIMARY KEY (run_id, task_id)
        //         );
        //         ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE task_results (
        //             run_id      INT NOT NULL,
        //             task_id     INT NOT NULL,
        //             result      JSONB NOT NULL,
        //             task_name   TEXT NOT NULL,
        //             attempt     INT NOT NULL,
        //             max_attempts    INT NOT NULL,
        //             function_name   TEXT NOT NULL,
        //             success         BOOL NOT NULL,
        //             stdout          TEXT NOT NULL,
        //             stderr          TEXT NOT NULL,
        //             template_args_str   TEXT NOT NULL,
        //             resolved_args_str   TEXT NOT NULL,
        //             started             TEXT NOT NULL,
        //             ended               TEXT NOT NULL,
        //             elapsed             INT NOT NULL,
        //             premature_failure   BOOL NOT NULL,
        //             premature_failure_error_str TEXT NOT NULL,
        //             timestamp timestamptz not null default NOW()
        //         );
        //         ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE task_statuses (
        //             run_id      INT NOT NULL,
        //             task_id     INT NOT NULL,
        //             status      TEXT NOT NULL,
        //             timestamp timestamptz not null default NOW()
        //         );
        //         ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE edges (
        //             run_id  INT,
        //             up      INT,
        //             down    INT,
        //             PRIMARY KEY (run_id, up, down)
        //         );
        //         ",
        // )
        // .execute(&client)
        // .await
        // .ok();
        // sqlx::query(
        //     "
        //         CREATE TABLE dep_keys (
        //             run_id INT,
        //             task_id INT,
        //             upstream_task_id INT,
        //             template_arg_key TEXT,
        //             upstream_result_key TEXT
        //         );
        //     ",
        // )
        // .execute(&client)
        // .await
        // .ok();

        // // Create an index for the `dag_id` column in the `dags` table
        // sqlx::query("CREATE INDEX idx_dags_dag_id ON dags (dag_id);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // // Create an index for the `dag_id` column in the `runs` table
        // sqlx::query("CREATE INDEX idx_runs_dag_id ON runs (run_id, dag_id);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // // Create an index for the `run_id` column in the `tasks` table
        // sqlx::query("CREATE INDEX idx_tasks_run_id ON tasks (run_id, task_id);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // // Create an index for the `task_id` column in the `task_results` table
        // sqlx::query("CREATE INDEX idx_task_results_task_id ON task_results (run_id, task_id);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // // Create an index for the `run_id` column in the `edges` table
        // sqlx::query("CREATE INDEX idx_edges_run_id ON edges (run_id, up);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // // Create an index for the `run_id` and `task_id` columns in the `dep_keys` table
        // sqlx::query("CREATE INDEX idx_dep_keys_run_id_task_id ON dep_keys (run_id, task_id, upstream_task_id);")
        //     .execute(&client)
        //     .await
        //     .ok();

        // client
    }

    #[timed(duration(printer = "debug!"))]
    pub fn contains_logical_date(
        dag_name: &str,
        dag_hash: &str,
        logical_date: DateTime<Utc>,
    ) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;
                let task = sqlx::query(
                    "SELECT EXISTS(SELECT 1 FROM runs WHERE dag_id = $1 AND dag_name = $2 AND logical_date = $3 )",
                )
                .bind(dag_hash)
                .bind(dag_name)
                .bind(logical_date)
                .fetch_one(&client)
                .await;
                task.unwrap().get(0)
            })
        })
    }
}

impl Runner for Db {
    //
    // #[timed(duration(printer = "info!"))]
    fn is_task_completed(&self, dag_run_id: &usize, task_id: &usize) -> bool {
        match self.get_task_status(dag_run_id, task_id) {
            TaskStatus::Pending | TaskStatus::Running | TaskStatus::Retrying => false,
            TaskStatus::Skipped | TaskStatus::Success => true,
            TaskStatus::Failure => {
                // let mut redis = Db::get_redis_client();
                // if redis
                //     .exists(format!("task_result:{dag_run_id}:{task_id}"))
                //     .unwrap()
                // {
                !self.get_task_result(dag_run_id, task_id).needs_retry()
                // }
                // false
            }
        }

        // if self.get_task_status(dag_run_id, task_id) == TaskStatus::Skipped {
        //     return true;
        // }

        // false
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_result(&self, dag_run_id: &usize, task_id: &usize) -> TaskResult {
        let mut redis = Db::get_redis_client();
        let result: Result<String, redis::RedisError> =
            redis.get(format!("task_result:{dag_run_id}:{task_id}"));

        if result.is_ok() {
            // println!("hit cache - task_result:{dag_run_id}:{task_id}");
            serde_json::from_str(&result.unwrap()).unwrap()
        } else {
            // println!("cache missed - task_result:{dag_run_id}:{task_id}");

            let res = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let client = Db::get_client().await;

                    let task = sqlx::query(
                        "
                    SELECT *
                    FROM task_results
                    WHERE run_id = $1 AND task_id = $2
                    ORDER BY timestamp DESC 
                    LIMIT 1;                    
                    ",
                    )
                    .bind(*dag_run_id as i32)
                    .bind(*task_id as i32)
                    .fetch_one(&client)
                    .await
                    .unwrap();
                    // let id: i32 = task.get("task_id");
                    // let name: String = task.get("name");
                    // let function_name: String = task.get("function_name");
                    // let template_args: Value =
                    //     serde_json::from_value(task.get("template_args")).unwrap();
                    // let options: TaskOptions = serde_json::from_value(task.get("options")).unwrap();
                    // let lazy_expand: bool = task.get("lazy_expand");
                    // let is_dynamic: bool = task.get("is_dynamic");
                    TaskResult {
                        task_id: task.get::<i32, _>("task_id") as usize,
                        result: serde_json::from_value(task.get("result")).unwrap(),
                        // task_name: task.get("task_name"),
                        attempt: task.get::<i32, _>("attempt") as usize,
                        max_attempts: task.get::<i32, _>("max_attempts") as isize,
                        function_name: task.get("function_name"),
                        success: task.get("success"),
                        stdout: task.get("stdout"),
                        stderr: task.get("stderr"),
                        template_args_str: task.get("template_args_str"),
                        resolved_args_str: task.get("resolved_args_str"),
                        started: task.get("started"),
                        ended: task.get("ended"),
                        elapsed: task.get::<i32, _>("elapsed") as i64,
                        premature_failure: task.get("premature_failure"),
                        premature_failure_error_str: task.get("premature_failure_error_str"),
                        is_branch: task.get("is_branch"),
                    }
                    // serde_json::from_value(v).unwrap()
                })
            });

            let _: Option<()> = redis
                .set(
                    format!("task_result:{dag_run_id}:{task_id}"),
                    serde_json::to_string(&res).unwrap(),
                )
                .ok();

            res
        }
    }

    #[timed(duration(printer = "info!"))]
    fn get_attempt_by_task_id(&self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                // Count the number of results for the given dag_run_id and task_id
                let result: i64 = sqlx::query(
                    "SELECT COUNT(*) FROM task_results WHERE run_id = $1 AND task_id = $2",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .fetch_one(&client)
                .await
                .unwrap()
                .get(0);

                result as usize + 1
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_status(&self, dag_run_id: &usize, task_id: &usize) -> TaskStatus {
        let mut status = TaskStatus::Pending.as_str().to_string();

        let mut redis = Db::get_redis_client();
        let result = redis.get(format!("task_status:{dag_run_id}:{task_id}"));

        let task_status = if result.is_ok() {
            // println!("hit cache - task_status:{dag_run_id}:{task_id}");
            result.unwrap()
        } else {
            // println!("cache missed - task_status:{dag_run_id}:{task_id}");

                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let client = Db::get_client().await;

                        // SORT
                        let s = sqlx::query(
                            "
                                    SELECT status 
                                    FROM task_statuses 
                                    WHERE run_id = $1 AND task_id = $2 
                                    ORDER BY timestamp DESC 
                                    LIMIT 1;
                                ",
                        )
                        .bind(*dag_run_id as i32)
                        .bind(*task_id as i32)
                        .fetch_optional(&client)
                        .await
                        .unwrap();

                        // Convert the string status to TaskStatus enum (assuming you have a function or method for that)
                        if let Some(s) = s {
                            status = s.get(0);
                        }
                    })
                });
            let _: Option<()> = redis
                    .set(format!("task_status:{dag_run_id}:{task_id}"), &status)
                .ok();

                status.to_string()
        };

        TaskStatus::from_str(&task_status).unwrap()
    }

    #[timed(duration(printer = "debug!"))]
    fn set_task_status(&mut self, dag_run_id: &usize, task_id: &usize, task_status: TaskStatus) {
        let mut redis = Db::get_redis_client();
        let _: Option<()> = redis
            .set(
                format!("task_status:{dag_run_id}:{task_id}"),
                task_status.as_str(),
            )
            .ok();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query(
                    "
                    INSERT INTO task_statuses (run_id, task_id, status)
                    VALUES ($1, $2, $3);
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .bind(task_status.as_str())
                .execute(&client)
                .await
                .unwrap(); // Be cautious with unwrap, consider proper error handling
            })
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
                let client = Db::get_client().await;

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
                // .execute(&client)
                // .await
                // .unwrap();

                let dag_run_id: i32 = sqlx::query(
                    "
                        INSERT INTO runs (dag_id, dag_name, logical_date)
                        VALUES ($1, $2, $3)
                        RETURNING run_id;                    
                    ",
                )
                .bind(dag_hash)
                .bind(dag_name)
                .bind(logical_date)
                .fetch_one(&client)
                .await
                .unwrap()
                .get(0);

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
                //     // .execute(&client).await.unwrap();
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

                dag_run_id as usize
            })
        })
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
    fn insert_task_results(&mut self, dag_run_id: &usize, result: &TaskResult) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query(
                    "
                    INSERT INTO task_results (
                        run_id, task_id, result, attempt, max_attempts, 
                        function_name, success, stdout, stderr, template_args_str, 
                        resolved_args_str, started, ended, elapsed, 
                        premature_failure, premature_failure_error_str, is_branch
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(result.task_id as i32)
                .bind(&result.result)
                // .bind(&result.task_name)
                .bind(result.attempt as i32)
                .bind(result.max_attempts as i32)
                .bind(&result.function_name)
                .bind(result.success)
                .bind(&result.stdout)
                .bind(&result.stderr)
                .bind(&result.template_args_str)
                .bind(&result.resolved_args_str)
                .bind(&result.started)
                .bind(&result.ended)
                .bind(result.elapsed)
                .bind(result.premature_failure)
                .bind(&result.premature_failure_error_str)
                .bind(result.is_branch)
                .execute(&client)
                .await
                .unwrap();
            })
        });

        let mut redis = Db::get_redis_client();
        let _: Option<()> = redis
            .set(
                format!("task_result:{dag_run_id}:{}", result.task_id),
                serde_json::to_string(result).unwrap(),
            )
            .ok();
    }

    #[timed(duration(printer = "debug!"))]
    fn mark_finished(&self, dag_run_id: &usize) {
        dbg!("mark finished!");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query("UPDATE runs SET status = $2 WHERE run_id = $1")
                    .bind(*dag_run_id as i32)
                    .bind("Completed")
                    // .bind(v)
                    .execute(&client)
                    .await
                    .unwrap();
            })
        });
    }

    #[timed(duration(printer = "info!"))]
    fn any_upstream_incomplete(&self, dag_run_id: &usize, task_id: &usize) -> bool {
        self.get_upstream(dag_run_id, task_id)
            .iter()
            .any(|edge| !self.is_task_completed(dag_run_id, edge))
    }

    #[timed(duration(printer = "info!"))]
    fn get_dependency_keys(
        &self,
        dag_run_id: &usize,
        task_id: &usize,
    ) -> HashMap<(usize, Option<String>), Option<String>> {
        let mut results: HashMap<(usize, Option<String>), Option<String>> = HashMap::new();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let rows = sqlx::query(
                    "
                        SELECT upstream_task_id, template_arg_key, upstream_result_key
                        FROM dep_keys
                        WHERE run_id = $1 AND task_id = $2;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .fetch_all(&client)
                .await
                .unwrap();

                for row in rows {
                    let upstream_task_id: i32 = row.get(0);
                    let template_arg_key = row.get(1);
                    let upstream_result_key: Option<String> = row.get(2);

                    results.insert(
                        (upstream_task_id as usize, template_arg_key),
                        upstream_result_key,
                    );
                }
            })
        });

        results
    }

    #[timed(duration(printer = "info!"))]
    fn get_downstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let rows = sqlx::query("SELECT down FROM edges WHERE run_id = $1 AND up = $2")
                    .bind(*dag_run_id as i32)
                    .bind(*task_id as i32)
                    .fetch_all(&client)
                    .await
                    .unwrap();

                let mut downstream_tasks = HashSet::new();

                for row in rows {
                    let downstream_task_id: i32 = row.get(0);
                    downstream_tasks.insert(downstream_task_id as usize);
                }

                downstream_tasks
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn remove_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query(
                    "
                        DELETE FROM edges 
                        WHERE run_id = $1 AND up = $2 AND down = $3;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(edge.0 as i32)
                .bind(edge.1 as i32)
                .execute(&client)
                .await
                .unwrap();

                sqlx::query(
                    "
                        DELETE FROM dep_keys 
                        WHERE run_id = $1 AND task_id = $2 AND upstream_task_id = $3 AND template_arg_key IS NULL;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(edge.1 as i32)
                .bind(edge.0 as i32)
                .execute(&client)
                .await
                .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query(
                    "
                            INSERT INTO edges (run_id, up, down)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (run_id, up, down)
                            DO NOTHING;
                        ",
                )
                .bind(*dag_run_id as i32)
                .bind(edge.0 as i32)
                .bind(edge.1 as i32)
                .execute(&client)
                .await
                .unwrap();
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_upstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let rows = sqlx::query("SELECT up FROM edges WHERE run_id = $1 AND down = $2")
                    .bind(*dag_run_id as i32)
                    .bind(*task_id as i32)
                    .fetch_all(&client)
                    .await
                    .unwrap();

                let mut upstream_tasks = HashSet::new();

                for row in rows {
                    let upstream_task_id: i32 = row.get(0);
                    upstream_tasks.insert(upstream_task_id as usize);
                }

                upstream_tasks
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_dependency_keys(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        upstream: (usize, Option<String>),
        v: Option<String>,
    ) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                sqlx::query(
                    "
                            INSERT INTO dep_keys (run_id, task_id, upstream_task_id, template_arg_key, upstream_result_key)
                            VALUES ($1, $2, $3, $4, $5);
                        ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .bind(upstream.0 as i32)
                .bind(upstream.1)
                .bind(v)
                .execute(&client)
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
    fn get_task_by_id(&self, dag_run_id: &usize, task_id: &usize) -> Task {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let task =
                    sqlx::query("SELECT * FROM tasks WHERE run_id = $1 AND task_id = $2 LIMIT 1")
                        .bind(*dag_run_id as i32)
                        .bind(*task_id as i32)
                        .fetch_one(&client)
                        .await
                        .unwrap();
                let id: i32 = task.get("task_id");
                // let name: String = task.get("name");
                let function_name: String = task.get("function_name");
                let template_args: Value =
                    serde_json::from_value(task.get("template_args")).unwrap();
                let options: TaskOptions = serde_json::from_value(task.get("options")).unwrap();
                let lazy_expand: bool = task.get("lazy_expand");
                let is_dynamic: bool = task.get("is_dynamic");
                let is_branch: bool = task.get("is_branch");

                Task {
                    id: id as usize,
                    function_name,
                    template_args,
                    options,
                    lazy_expand,
                    is_dynamic,
                    is_branch,
                }
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
                let client = Db::get_client().await;

                let q: i32 = sqlx::query(
                    "
                        INSERT INTO tasks (run_id, task_id, function_name, template_args, options, lazy_expand, is_dynamic, is_branch)
                        VALUES (
                            $1,
                            (SELECT COALESCE(MAX(task_id) + 1, 0) FROM tasks WHERE run_id = $1),
                            $2, $3, $4, $5, $6, $7
                        )
                        RETURNING task_id;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(function_name)
                .bind(template_args)
                .bind(serde_json::to_value(options).unwrap())
                .bind(lazy_expand)
                .bind(is_dynamic)
                .bind(is_branch)
                .fetch_one(&client).await.unwrap().get(0);

                self.set_task_status(dag_run_id, &(q as usize), TaskStatus::Pending);

                q as usize
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_template_args(&self, dag_run_id: &usize, task_id: &usize) -> serde_json::Value {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let k = sqlx::query(
                    "
                    SELECT template_args
                    FROM tasks
                    WHERE run_id = $1 AND task_id = $2
                    LIMIT 1;                    
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .fetch_optional(&client)
                .await;

                // if k.is_err() {
                //     // dbg!(&dag_run_id, &task_id);
                //     panic!();
                // }

                k.unwrap().unwrap().get(0)
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn set_template_args(&mut self, dag_run_id: &usize, task_id: &usize, template_args_str: &str) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;
                let v: Value = serde_json::from_str(template_args_str).unwrap();

                sqlx::query(
                    "UPDATE tasks SET template_args = $3 WHERE run_id = $1 AND task_id = $2",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .bind(v)
                .execute(&client)
                .await
                .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn new(name: &str, nodes: &[Task], edges: &HashSet<(usize, usize)>) -> Self {
        Self {
            name: name.into(),
            edges: edges.clone(),
            nodes: nodes.to_vec(),
        }
    }

    #[timed(duration(printer = "debug!"))]
    fn get_default_edges(&self) -> HashSet<(usize, usize)> {
        self.edges.clone()
    }

    #[timed(duration(printer = "info!"))]
    fn set_status_to_running_if_possible(&mut self, dag_run_id: &usize, task_id: &usize) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                let mut redis = Db::get_redis_client();
                let _: Option<()> = redis
                    .set(format!("task_status:{dag_run_id}:{task_id}"), "Running")
                    .ok();

                // Update the task status to "Running" if its last status is "Pending" or "Retrying"
                let rows_affected = sqlx::query(
                    "
                    UPDATE task_statuses
                    SET status = 'Running'
                    WHERE run_id = $1 AND task_id = $2 AND (
                        SELECT status 
                        FROM task_statuses
                        WHERE run_id = $1 AND task_id = $2
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) IN ('Pending', 'Retrying');
                    
                ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .execute(&client)
                .await
                .unwrap()
                .rows_affected();

                rows_affected > 0
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_dag_name(&self) -> String {
        self.name.clone()
    }

    #[timed(duration(printer = "debug!"))]
    fn get_all_tasks(&self, dag_run_id: &usize) -> Vec<Task> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let client = Db::get_client().await;

                // TODO sort?
                let tasks = sqlx::query("SELECT * FROM tasks WHERE run_id = $1")
                    .bind(*dag_run_id as i32)
                    .fetch_all(&client)
                    .await
                    .unwrap();

                tasks
                    .iter()
                    .map(|task| {
                        let id: i32 = task.get("task_id");
                        let function_name: String = task.get("function_name");
                        let template_args: Value =
                            serde_json::from_value(task.get("template_args")).unwrap();
                        let options: TaskOptions =
                            serde_json::from_value(task.get("options")).unwrap();
                        let lazy_expand: bool = task.get("lazy_expand");
                        let is_dynamic: bool = task.get("is_dynamic");
                        let is_branch: bool = task.get("is_branch");
                        Task {
                            id: id as usize,
                            function_name,
                            template_args,
                            options,
                            lazy_expand,
                            is_dynamic,
                            is_branch,
                        }
                    })
                    .collect()
            })
        })
    }
}
