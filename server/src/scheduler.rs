use std::{collections::{HashSet, HashMap}, time::Duration, sync::{Arc, Mutex}};

use chrono::{DateTime, Utc};
use runner::local::hash_dag;
use saffron::Cron;
use task::task::Task;
use thepipelinetool::prelude::DagOptions;
use tokio::time::sleep;

use crate::{_get_dags, _get_edges, _get_options, _get_tasks, _run, db::Db};

pub fn scheduler(up_to: &DateTime<Utc>) {
    let up_to_initial = up_to.clone();

    let last_checked: Arc<Mutex<HashMap<String, DateTime<Utc>>>> = Arc::new(Mutex::new(HashMap::new()));

    tokio::spawn(async move {

        loop {
            let dags = _get_dags();

            'outer: for dag_name in dags {
                let options: DagOptions = serde_json::from_value(_get_options(&dag_name)).unwrap();
                let up_to = **last_checked.lock().unwrap().get(&dag_name).get_or_insert(&up_to_initial);
                if let Some(schedule) = &options.schedule {
                    match schedule.parse::<Cron>() {
                        Ok(cron) => {
                            if !cron.any() {
                                println!("Cron will never match any given time!");
                                return;
                            }

                            if let Some(start_date) = options.start_date {
                                if start_date >= up_to {
                                    break 'outer;
                                }
                            }

                            if let Some(end_date) = options.end_date {
                                if end_date <= up_to {
                                    break 'outer;
                                }
                            }

                            let futures = cron.clone().iter_from(up_to);
                            let nodes: Vec<Task> =
                                serde_json::from_value(_get_tasks(&dag_name)).unwrap();
                            let edges: HashSet<(usize, usize)> =
                                serde_json::from_value(_get_edges(&dag_name)).unwrap();

                            let hash = hash_dag(
                                &serde_json::to_string(&nodes).unwrap(),
                                &edges.iter().collect::<Vec<&(usize, usize)>>(),
                            );

                            'inner: for time in futures {
                                if !cron.contains(time) {
                                    println!("Failed check! Cron does not contain {}.", time);
                                    break 'inner;
                                }
                                if time >= Utc::now() {
                                    break 'inner;
                                }
                                if let Some(end_date) = options.end_date {
                                    if time > end_date {
                                        break 'inner;
                                    }
                                }
                                // check if date is already in db
                                if Db::contains_logical_date(&dag_name, &hash, time) {
                                    continue 'inner;
                                }

                                last_checked.lock().unwrap().insert(dag_name.clone(), Utc::now());

                                _run(&dag_name, time);
                                println!("scheduling {} {dag_name}", time.format("%F %R"));
                            }
                        }
                        Err(err) => println!("{err}: {schedule}"),
                    }
                }
            }

            // TODO read from env
            sleep(Duration::new(5, 0)).await;
        }
    });
}
