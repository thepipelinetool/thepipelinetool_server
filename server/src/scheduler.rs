use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use runner::local::hash_dag;
use saffron::Cron;
use task::task::Task;
use thepipelinetool::prelude::DagOptions;
use tokio::time::sleep;

use crate::{_get_dags, _get_default_edges, _get_options, _get_default_tasks, _trigger_run, db::Db};

pub fn scheduler(up_to: &DateTime<Utc>) {
    let up_to_initial = up_to.clone();

    let last_checked: Arc<Mutex<HashMap<String, DateTime<Utc>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    tokio::spawn(async move {
        loop {
            let dags = _get_dags();

            for dag_name in dags {
                let last_checked = last_checked.clone();
                tokio::spawn(async move {
                    let options: DagOptions =
                        serde_json::from_value(_get_options(&dag_name)).unwrap();
                    let up_to = **last_checked
                        .lock()
                        .unwrap()
                        .get(&dag_name)
                        .get_or_insert(&up_to_initial);

                    last_checked.lock().unwrap().insert(dag_name.clone(), Utc::now());

                    if let Some(schedule) = &options.schedule {
                        match schedule.parse::<Cron>() {
                            Ok(cron) => {
                                if !cron.any() {
                                    println!("Cron will never match any given time!");
                                    return;
                                }
                                // println!("checking for schedules: {dag_name} {up_to}");

                                if let Some(end_date) = options.end_date {
                                    if end_date <= up_to {
                                        return;
                                    }
                                }

                                if let Some(start_date) = options.start_date {
                                    if start_date >= up_to {
                                        return;
                                    }
                                }

                                let futures = cron.clone().iter_from(up_to);
                                let nodes: Vec<Task> =
                                    serde_json::from_value(_get_default_tasks(&dag_name)).unwrap();
                                let edges: HashSet<(usize, usize)> =
                                    serde_json::from_value(_get_default_edges(&dag_name)).unwrap();

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

                                    _trigger_run(&dag_name, time);
                                    println!("scheduling {} {dag_name}", time.format("%F %R"));
                                }
                            }
                            Err(err) => println!("{err}: {schedule}"),
                        }
                    }

                });
            }

            // TODO read from env
            sleep(Duration::new(1, 0)).await;
        }
    });
}
