use std::collections::HashSet;

use chrono::{Utc, DateTime};
use runner::local::hash_dag;
use saffron::Cron;
use task::task::Task;
use thepipelinetool::prelude::DagOptions;

use crate::{_get_dags, _get_edges, _get_options, _get_tasks, _run, db::Db};

pub fn catchup(up_to: &DateTime<Utc>) {
    let up_to: DateTime<Utc> = up_to.clone();
    // let up_to = up_to.clone();
    tokio::spawn(async move {
        let dags = _get_dags();

        'outer: for dag_name in dags {
            println!("checking for catchup: {dag_name}");
            let options: DagOptions = serde_json::from_value(_get_options(&dag_name)).unwrap();
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

                        let futures =
                            cron.clone()
                                .iter_from(if let Some(start_date) = options.start_date {
                                    if options.catchup {
                                        start_date.into()
                                    } else {
                                        up_to
                                    }
                                } else {
                                    up_to
                                });
                        let nodes: Vec<Task> =
                            serde_json::from_value(_get_tasks(&dag_name)).unwrap();
                        let edges: HashSet<(usize, usize)> =
                            serde_json::from_value(_get_edges(&dag_name)).unwrap();

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

                            if Db::contains_logical_date(&dag_name, &hash, time) {
                                continue 'inner;
                            }

                            _run(&dag_name, time);
                            println!("scheduling catchup {dag_name} {}", time.format("%F %R"));
                        }
                    }
                    Err(err) => println!("{err}: {schedule}"),
                }
            }
        }
    });
}
