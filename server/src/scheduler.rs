use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

use saffron::Cron;
use thepipelinetool::prelude::DagOptions;
use tokio::time::sleep;

use crate::{_get_dags, _get_hash, _get_options, _trigger_run, db::Db};

pub fn scheduler(up_to: &DateTime<Utc>, pool: Pool<Postgres>) {
    let up_to_initial = *up_to;

    let last_checked: Arc<Mutex<HashMap<String, DateTime<Utc>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    // let pool = Arc::new(Mutex::new(pool));

    tokio::spawn(async move {
        loop {
            let dags = _get_dags();

            for dag_name in dags {
                let last_checked = last_checked.clone();
                let pool: Pool<Postgres> = pool.clone();

                tokio::spawn(async move {
                    let options: DagOptions =
                        serde_json::from_str(&_get_options(&dag_name).await).unwrap();
                    let up_to = **last_checked
                        .lock()
                        .unwrap()
                        .get(&dag_name)
                        .get_or_insert(&up_to_initial);

                    last_checked
                        .lock()
                        .unwrap()
                        .insert(dag_name.clone(), Utc::now());

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
                                    if Db::contains_logical_date(
                                        &dag_name,
                                        &_get_hash(&dag_name).await,
                                        time,
                                        pool.clone(),
                                    ) {
                                        continue 'inner;
                                    }

                                    _trigger_run(&dag_name, time, pool.clone()).await;
                                    println!("scheduling {} {dag_name}", time.format("%F %R"));
                                }
                            }
                            Err(err) => println!("{err}: {schedule}"),
                        }
                    }
                });
            }

            // TODO read from env
            sleep(Duration::new(5, 0)).await;
        }
    });
}
