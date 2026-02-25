#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Instant;

use gitaly_proto::gitaly::ServerInfoRequest;

const BURST_SIZE: usize = 64;
const ROUNDS: usize = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let server = support::start_test_server("stress-server-info").await;
    let client = support::connect_server_client(&server.endpoint).await;
    let request = ServerInfoRequest::default();

    let started = Instant::now();
    let mut completed = 0usize;

    for _round in 0..ROUNDS {
        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..BURST_SIZE {
            let mut task_client = client.clone();
            let task_request = request.clone();
            join_set.spawn(async move {
                task_client
                    .server_info(task_request)
                    .await
                    .expect("server_info request should succeed")
                    .into_inner()
            });
        }

        while let Some(joined) = join_set.join_next().await {
            let info = joined.expect("worker task should join");
            assert!(
                !info.server_version.is_empty(),
                "server version should remain available under load"
            );
            assert!(
                !info.git_version.is_empty(),
                "git version should remain available under load"
            );
            completed += 1;
        }
    }

    let elapsed = started.elapsed();
    let elapsed_ms = elapsed.as_millis().max(1);
    let rps = (completed as f64 * 1000.0) / elapsed_ms as f64;
    println!(
        "stress_gate bench=stress_server_info requests={completed} elapsed_ms={elapsed_ms} rps={rps:.2}"
    );

    server.shutdown().await;
}
