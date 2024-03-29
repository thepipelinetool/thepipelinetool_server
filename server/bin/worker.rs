use server::{_get_dag_path_by_name, get_redis_pool, redis_runner::RedisRunner};
use std::time::Duration;
use thepipelinetool::server::*;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let pool = get_redis_pool();
    let mut dummy = RedisRunner::dummy(pool.clone());

    loop {
        if let Some(ordered_queued_task) = dummy.pop_priority_queue() {
            let mut runner = RedisRunner::from_local_dag(
                &ordered_queued_task.queued_task.dag_name,
                pool.clone(),
            );
            runner.work(
                ordered_queued_task.queued_task.run_id,
                &ordered_queued_task,
                _get_dag_path_by_name(&ordered_queued_task.queued_task.dag_name),
            );
            runner.remove_from_temp_queue(&ordered_queued_task.queued_task);
        } else {
            sleep(Duration::new(2, 0)).await;
        }
    }
}
