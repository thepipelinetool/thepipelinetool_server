use log::debug;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use redis::{Commands, Connection};
use serde_json::Value;
use thepipelinetool::prelude::*;

// use task::task_options::TaskOptions;
// use task::{task::Task, task_result::TaskResult, task_status::TaskStatus};

// #[derive(Clone)]
pub struct Db {
    edges: HashSet<(usize, usize)>,
    nodes: Vec<Task>,
    name: String,
    pool: Pool<Postgres>,
    redis: Connection,
}

// impl Default for Postgres {
//     fn default() -> Self {
//         Self {}
//     }
// }

use sqlx::{Pool, Postgres, Row};

use timed::timed;

use crate::get_redis_client;
// use task::task::Task;
// use task::task_options::TaskOptions;
// use task::task_result::TaskResult;
// use task::task_status::TaskStatus;
// use thepipelinetool::prelude::*;

// #[derive(Serialize)]
pub struct Run {
    pub run_id: usize,
    pub date: DateTime<Utc>,
}

impl Db {
    #[timed(duration(printer = "debug!"))]
    pub fn new(
        name: &str,
        nodes: &[Task],
        edges: &HashSet<(usize, usize)>,
        pool: Pool<Postgres>,
        // redis: Connection,
    ) -> Self {
        let redis = get_redis_client();
        Self {
            name: name.into(),
            edges: edges.clone(),
            nodes: nodes.to_vec(),
            pool,
            redis,
        }
    }

    pub async fn get_all_results(
        run_id: usize,
        task_id: usize,
        pool: Pool<Postgres>,
    ) -> Vec<TaskResult> {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //         //

        //         let tasks = self.get_
        //     })
        // })

        //    let db =  Db::new(&"", &[], &HashSet::new(), pool);

        //    db.get_a

        //    todo!()

        let rows = sqlx::query(
            "
            SELECT *
            FROM task_results
            WHERE run_id = $1 AND task_id = $2
            ORDER BY timestamp DESC;                    
            ",
        )
        .bind(run_id as i32)
        .bind(task_id as i32)
        .fetch_all(&pool)
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

        let mut results = vec![];
        for row in rows {
            results.push(TaskResult {
                task_id: row.get::<i32, _>("task_id") as usize,
                result: serde_json::from_value(row.get("result")).unwrap(),
                // task_name: task.get("task_name"),
                attempt: row.get::<i32, _>("attempt") as usize,
                max_attempts: row.get::<i32, _>("max_attempts") as isize,
                function_name: row.get("function_name"),
                success: row.get("success"),
                // stdout: task.get("stdout"),
                // stderr: task.get("stderr"),
                // template_args_str: task.get("template_args_str"),
                resolved_args_str: row.get("resolved_args_str"),
                started: row.get("started"),
                ended: row.get("ended"),
                elapsed: row.get::<i32, _>("elapsed") as i64,
                premature_failure: row.get("premature_failure"),
                premature_failure_error_str: row.get("premature_failure_error_str"),
                is_branch: row.get("is_branch"),
            });
        }
        results
    }

    // #[timed(duration(printer = "debug!"))]
    pub async fn get_runs(dag_name: &str, pool: Pool<Postgres>) -> Vec<Run> {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //

        let rows =
            sqlx::query("SELECT run_id, logical_date, timestamp FROM runs WHERE dag_name = $1")
                .bind(dag_name)
                .fetch_all(&pool)
                .await
                .unwrap();

        let mut v = vec![];
        for row in rows {
            // let mut value = json!({});

            let run_id: i32 = row.get(0);
            // value["run_id"] = run_id.to_string().into();

            let date: DateTime<Utc> = row.get(1);
            // value["date"] = date.to_string().into();

            v.push(Run {
                run_id: run_id as usize,
                date,
            });
        }

        v
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
    #[timed(duration(printer = "debug!"))]
    pub async fn get_pending_runs(dag_name: &str, pool: Pool<Postgres>) -> Vec<(usize, String)> {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        // TODO get only incomplete
        let rows =
            sqlx::query("SELECT run_id, dag_id FROM runs WHERE dag_name = $1 AND status = $2")
                .bind(dag_name)
                .bind("Pending")
                .fetch_all(&pool)
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
        //     })
        // })
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn init_tables(pool: Pool<Postgres>) {
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
        // .execute(&self.pool)
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
        .execute(&pool)
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
        .execute(&pool)
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
        .execute(&pool)
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
        .execute(&pool)
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
        .execute(&pool)
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
        .execute(&pool)
        .await
        .ok();

        sqlx::query(
            "
            CREATE TABLE logs (
                run_id INT,
                task_id INT,
                attempt INT,
                log TEXT
            );
        ",
        )
        .execute(&pool)
        .await
        .ok();
    }

    #[timed(duration(printer = "debug!"))]
    pub async fn contains_logical_date(
        dag_name: &str,
        dag_hash: &str,
        logical_date: DateTime<Utc>,
        pool: Pool<Postgres>,
    ) -> bool {
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
        //
        let task = sqlx::query(
                    "SELECT EXISTS(SELECT 1 FROM runs WHERE dag_id = $1 AND dag_name = $2 AND logical_date = $3 )",
                )
                .bind(dag_hash)
                .bind(dag_name)
                .bind(logical_date)
                .fetch_one(&pool)
                .await;
        task.unwrap().get(0)
        //     })
        // })
    }
}

impl Runner for Db {
    #[timed(duration(printer = "debug!"))]
    fn get_log(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        attempt: usize,
        // pool: Pool<Postgres>,
    ) -> String {
        let key = format!("task_log:{dag_run_id}:{task_id}:{attempt}");

        self.redis.get(&key).unwrap_or("".into())


        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(async {
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
        //     })
        // })
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

    fn handle_log(
        &mut self,
        dag_run_id: &usize,
        task_id: &usize,
        attempt: usize,
    ) -> Box<dyn Fn(String) + Send> {
        // let task_logs = self.task_logs.clone();
        let task_id = *task_id;
        let dag_run_id = *dag_run_id;
        // let pool = self.pool.clone();

        Box::new(move |s| {
            let key = format!("task_log:{dag_run_id}:{task_id}:{attempt}");

            // if let Result(bool)self.redis.exists(key) {
            //     let _: Result<(), redis::RedisError> = self.redis.set(key, "");
            // }
            let mut redis = get_redis_client();
            let prev: String = redis.get(&key).unwrap_or("".into());
            let _: Result<(), redis::RedisError> = redis.append(key, format!("{prev}{s}"));
        })

        // Box::new(move |s| {
        //     // let mut task_logs = task_logs.lock().unwrap();
        //     // if !task_logs.contains_key(&task_id) {
        //     //     task_logs.insert(task_id, s);
        //     // } else {
        //     //     *task_logs.get_mut(&task_id).unwrap() += &s;
        //     // }
        //     // let redis = get_redis_client();
        //     // tokio::task::block_in_place(|| {
        //     tokio::runtime::Runtime::new().unwrap().block_on(async {
        //         sqlx::query(
        //             "UPDATE logs
        //             SET log = log || $4
        //             WHERE run_id = $1 AND task_id = $2 AND attempt = $3",
        //         )
        //         .bind(dag_run_id as i32)
        //         .bind(task_id as i32)
        //         .bind(attempt as i32)
        //         .bind(s)
        //         .execute(&pool)
        //         .await
        //         .unwrap();
        //     });
        //     // });
        // })
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
    //     //     .exists(format!("task_result:{dag_run_id}:{task_id}"))
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

    #[timed(duration(printer = "debug!"))]
    fn set_status_to_running_if_possible(&mut self, dag_run_id: &usize, task_id: &usize) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // let mut redis = Db::get_redis_client();
                let _: Result<(), redis::RedisError> = self
                    .redis
                    .set(format!("task_status:{dag_run_id}:{task_id}"), "Running");

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
                .execute(&self.pool)
                .await
                .unwrap()
                .rows_affected();

                rows_affected > 0
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_result(&mut self, dag_run_id: &usize, task_id: &usize) -> TaskResult {
        // let mut redis = Db::get_redis_client();
        let result: Result<String, redis::RedisError> = self
            .redis
            .get(format!("task_result:{dag_run_id}:{task_id}"));

        if result.is_ok() {
            println!("hit cache - task_result:{dag_run_id}:{task_id}");
            serde_json::from_str(&result.unwrap()).unwrap()
        } else {
            println!("cache missed - task_result:{dag_run_id}:{task_id}");

            let res = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
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
                    .fetch_one(&self.pool)
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
                        // stdout: task.get("stdout"),
                        // stderr: task.get("stderr"),
                        // template_args_str: task.get("template_args_str"),
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

            let _: Result<(), redis::RedisError> = self.redis.set(
                format!("task_result:{dag_run_id}:{task_id}"),
                serde_json::to_string(&res).unwrap(),
            );

            res
        }
    }

    #[timed(duration(printer = "debug!"))]
    fn get_attempt_by_task_id(&self, dag_run_id: &usize, task_id: &usize) -> usize {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Count the number of results for the given dag_run_id and task_id
                let result: i64 = sqlx::query(
                    "SELECT COUNT(*) FROM task_results WHERE run_id = $1 AND task_id = $2",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .fetch_one(&self.pool)
                .await
                .unwrap()
                .get(0);

                result as usize + 1
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_status(&mut self, dag_run_id: &usize, task_id: &usize) -> TaskStatus {
        let mut status = TaskStatus::Pending.as_str().to_string();

        // let mut redis = Db::get_redis_client();
        let result = self
            .redis
            .get(format!("task_status:{dag_run_id}:{task_id}"));

        let task_status = if result.is_ok() {
            println!("hit cache - task_status:{dag_run_id}:{task_id}");
            result.unwrap()
        } else {
            println!("cache missed - task_status:{dag_run_id}:{task_id}");

            result.unwrap_or_else(|_| {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
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
                        .fetch_optional(&self.pool)
                        .await
                        .unwrap();

                        // Convert the string status to TaskStatus enum (assuming you have a function or method for that)
                        if let Some(s) = s {
                            status = s.get(0);
                        }
                    })
                });
                let _: Result<(), redis::RedisError> = self
                    .redis
                    .set(format!("task_status:{dag_run_id}:{task_id}"), &status);

                status.to_string()
            })
        };

        TaskStatus::from_str(&task_status).unwrap()
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
        let _: Result<(), redis::RedisError> = self.redis.set(
            format!("task_status:{dag_run_id}:{task_id}"),
            task_status.as_str(),
        );

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "
                    INSERT INTO task_statuses (run_id, task_id, status)
                    VALUES ($1, $2, $3);
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .bind(task_status.as_str())
                .execute(&self.pool)
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
                .fetch_one(&self.pool)
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

                dag_run_id as usize
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_task_results(&mut self, dag_run_id: &usize, result: &TaskResult) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "
                    INSERT INTO task_results (
                        run_id, task_id, result, attempt, max_attempts, 
                        function_name, success, resolved_args_str, started, ended, elapsed, 
                        premature_failure, premature_failure_error_str, is_branch
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
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
                // .bind(&result.stdout)
                // .bind(&result.stderr)
                // .bind(&result.template_args_str)
                .bind(&result.resolved_args_str)
                .bind(&result.started)
                .bind(&result.ended)
                .bind(result.elapsed)
                .bind(result.premature_failure)
                .bind(&result.premature_failure_error_str)
                .bind(result.is_branch)
                .execute(&self.pool)
                .await
                .unwrap();
            })
        });

        // let mut redis = Db::get_redis_client();
        let _: Result<(), redis::RedisError> = self.redis.set(
            format!("task_result:{dag_run_id}:{}", result.task_id),
            serde_json::to_string(result).unwrap(),
        );
    }

    #[timed(duration(printer = "debug!"))]
    fn mark_finished(&self, dag_run_id: &usize) {
        dbg!("mark finished!");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query("UPDATE runs SET status = $2 WHERE run_id = $1")
                    .bind(*dag_run_id as i32)
                    .bind("Completed")
                    // .bind(v)
                    .execute(&self.pool)
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
    ) -> HashMap<(usize, Option<String>), Option<String>> {
        let mut results: HashMap<(usize, Option<String>), Option<String>> = HashMap::new();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows = sqlx::query(
                    "
                        SELECT upstream_task_id, template_arg_key, upstream_result_key
                        FROM dep_keys
                        WHERE run_id = $1 AND task_id = $2;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .fetch_all(&self.pool)
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
                .execute(&self.pool)
                .await
                .unwrap();
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_downstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows = sqlx::query("SELECT down FROM edges WHERE run_id = $1 AND up = $2")
                    .bind(*dag_run_id as i32)
                    .bind(*task_id as i32)
                    .fetch_all(&self.pool)
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
    fn get_upstream(&self, dag_run_id: &usize, task_id: &usize) -> HashSet<usize> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let rows = sqlx::query("SELECT up FROM edges WHERE run_id = $1 AND down = $2")
                    .bind(*dag_run_id as i32)
                    .bind(*task_id as i32)
                    .fetch_all(&self.pool)
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
    fn remove_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query(
                    "
                        DELETE FROM edges 
                        WHERE run_id = $1 AND up = $2 AND down = $3;
                    ",
                )
                .bind(*dag_run_id as i32)
                .bind(edge.0 as i32)
                .bind(edge.1 as i32)
                .execute(&self.pool)
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
                .execute(&self.pool)
                .await
                .unwrap();
            })
        });
    }

    #[timed(duration(printer = "debug!"))]
    fn insert_edge(&mut self, dag_run_id: &usize, edge: &(usize, usize)) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
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
                .execute(&self.pool)
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
                let tasks = sqlx::query("SELECT * FROM tasks WHERE run_id = $1")
                    .bind(*dag_run_id as i32)
                    .fetch_all(&self.pool)
                    .await
                    .unwrap();

                tasks
                    .iter()
                    // .filter(|t| t.get)
                    .filter_map(|task| {
                        let id: i32 = task.get("task_id");
                        if self.is_task_completed(dag_run_id, &(id as usize)) {
                            return None;
                        }

                        let function_name: String = task.get("function_name");
                        let template_args: Value =
                            serde_json::from_value(task.get("template_args")).unwrap();
                        let options: TaskOptions =
                            serde_json::from_value(task.get("options")).unwrap();
                        let lazy_expand: bool = task.get("lazy_expand");
                        let is_dynamic: bool = task.get("is_dynamic");
                        let is_branch: bool = task.get("is_branch");
                        Some(Task {
                            id: id as usize,
                            function_name,
                            template_args,
                            options,
                            lazy_expand,
                            is_dynamic,
                            is_branch,
                        })
                    })
                    // .filter(|t| )
                    .collect()
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_all_tasks(&self, dag_run_id: &usize) -> Vec<Task> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // TODO sort?
                let tasks = sqlx::query("SELECT * FROM tasks WHERE run_id = $1")
                    .bind(*dag_run_id as i32)
                    .fetch_all(&self.pool)
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

    #[timed(duration(printer = "debug!"))]
    fn get_default_edges(&self) -> HashSet<(usize, usize)> {
        self.edges.clone()
    }

    #[timed(duration(printer = "debug!"))]
    fn get_task_by_id(&self, dag_run_id: &usize, task_id: &usize) -> Task {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let task =
                    sqlx::query("SELECT * FROM tasks WHERE run_id = $1 AND task_id = $2 LIMIT 1")
                        .bind(*dag_run_id as i32)
                        .bind(*task_id as i32)
                        .fetch_one(&self.pool)
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
                .fetch_one(&self.pool).await.unwrap().get(0);

                self.set_task_status(dag_run_id, &(q as usize), TaskStatus::Pending);

                q as usize
            })
        })
    }

    #[timed(duration(printer = "debug!"))]
    fn get_template_args(&self, dag_run_id: &usize, task_id: &usize) -> serde_json::Value {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
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
                .fetch_optional(&self.pool)
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
                let v: Value = serde_json::from_str(template_args_str).unwrap();

                sqlx::query(
                    "UPDATE tasks SET template_args = $3 WHERE run_id = $1 AND task_id = $2",
                )
                .bind(*dag_run_id as i32)
                .bind(*task_id as i32)
                .bind(v)
                .execute(&self.pool)
                .await
                .unwrap();
            })
        });
    }
}
