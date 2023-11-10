use std::collections::HashSet;

use chrono::{DateTime, Utc};
// use runner::local::hash_dag;
use saffron::Cron;
use sqlx::{Pool, Postgres};
// use task::task::Task;
use thepipelinetool::prelude::*;

use crate::{
    _get_dags, _get_default_edges, _get_default_tasks, _get_options, _trigger_run, db::Db,
};

pub fn catchup(up_to: &DateTime<Utc>, pool: Pool<Postgres>) {
    let up_to: DateTime<Utc> = *up_to;
    // let up_to = up_to.clone();
    tokio::spawn(async move {
        let dags = _get_dags();

        for dag_name in dags {
            let pool: Pool<Postgres> = pool.clone();

            tokio::spawn(async move {
                let options: DagOptions = serde_json::from_value(_get_options(&dag_name)).unwrap();
                if let Some(schedule) = &options.schedule {
                    match schedule.parse::<Cron>() {
                        Ok(cron) => {
                            if !cron.any() {
                                println!("Cron will never match any given time!");
                                return;
                            }
                            println!("checking for catchup: {dag_name}");

                            if let Some(start_date) = options.start_date {
                                if start_date >= up_to {
                                    return;
                                }
                            }

                            let futures = cron.clone().iter_from(
                                if let Some(start_date) = options.start_date {
                                    if options.catchup {
                                        start_date.into()
                                    } else {
                                        up_to
                                    }
                                } else {
                                    up_to
                                },
                            );
                            let nodes: Vec<Task> =
                                serde_json::from_value(_get_default_tasks(&dag_name)).unwrap();
                            let edges: HashSet<(usize, usize)> =
                                serde_json::from_value(_get_default_edges(&dag_name)).unwrap();

                            let hash = hash_dag(
                                &serde_json::to_string(&nodes).unwrap(),
                                &edges.iter().collect::<Vec<&(usize, usize)>>(),
                            );

                            // remove take 10
                            'inner: for time in futures {
                                if !cron.contains(time) {
                                    println!("Failed check! Cron does not contain {}.", time);
                                    break 'inner;
                                }
                                if time >= up_to {
                                    break 'inner;
                                }
                                if let Some(end_date) = options.end_date {
                                    if time > end_date {
                                        break 'inner;
                                    }
                                }
                                // check if date is already in db

                                if Db::contains_logical_date(&dag_name, &hash, time, pool.clone()) {
                                    continue 'inner;
                                }

                                _trigger_run(&dag_name, time, pool.clone());
                                println!("scheduling catchup {dag_name} {}", time.format("%F %R"));
                            }
                        }
                        Err(err) => println!("{err}: {schedule}"),
                    }
                }
            });
        }
    });
}
