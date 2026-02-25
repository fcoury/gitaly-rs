#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Instant;

use gitaly_proto::gitaly::{Repository, RepositoryExistsRequest};

const BURST_SIZE: usize = 64;
const ROUNDS: usize = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let server = support::start_test_server("stress-repository-exists").await;
    let repository_relative_path = "bench/project.git";
    std::fs::create_dir_all(server.storage_dir().join(repository_relative_path))
        .expect("repository path should be creatable");

    let client = support::connect_repository_client(&server.endpoint).await;
    let request = RepositoryExistsRequest {
        repository: Some(Repository {
            storage_name: "default".to_string(),
            relative_path: repository_relative_path.to_string(),
            ..Repository::default()
        }),
    };

    let started = Instant::now();
    let mut completed = 0usize;

    for _round in 0..ROUNDS {
        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..BURST_SIZE {
            let mut task_client = client.clone();
            let task_request = request.clone();
            join_set.spawn(async move {
                task_client
                    .repository_exists(task_request)
                    .await
                    .expect("repository_exists request should succeed")
                    .into_inner()
                    .exists
            });
        }

        while let Some(joined) = join_set.join_next().await {
            let exists = joined.expect("worker task should join");
            assert!(exists, "repository should exist for stress baseline");
            completed += 1;
        }
    }

    let elapsed = started.elapsed();
    println!(
        "stress_repository_exists baseline: requests={completed} elapsed_ms={}",
        elapsed.as_millis()
    );

    server.shutdown().await;
}
