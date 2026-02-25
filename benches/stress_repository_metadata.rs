#[path = "../tests/support/mod.rs"]
mod support;

use std::time::Instant;

use gitaly_proto::gitaly::{
    CreateRepositoryRequest, ObjectFormat, ObjectFormatRequest, Repository,
};

const BURST_SIZE: usize = 64;
const ROUNDS: usize = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let server = support::start_test_server("stress-repository-metadata").await;
    let repository_relative_path = "bench-metadata/project.git";

    let repository = Repository {
        storage_name: "default".to_string(),
        relative_path: repository_relative_path.to_string(),
        ..Repository::default()
    };

    let mut setup_client = support::connect_repository_client(&server.endpoint).await;
    setup_client
        .create_repository(CreateRepositoryRequest {
            repository: Some(repository.clone()),
            ..CreateRepositoryRequest::default()
        })
        .await
        .expect("create_repository request should succeed");

    let client = support::connect_repository_client(&server.endpoint).await;
    let request = ObjectFormatRequest {
        repository: Some(repository),
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
                    .object_format(task_request)
                    .await
                    .expect("object_format request should succeed")
                    .into_inner()
                    .format
            });
        }

        while let Some(joined) = join_set.join_next().await {
            let format = joined.expect("worker task should join");
            assert_eq!(
                format,
                ObjectFormat::Sha1 as i32,
                "object_format should stay stable for benchmark repository"
            );
            completed += 1;
        }
    }

    let elapsed = started.elapsed();
    let elapsed_ms = elapsed.as_millis().max(1);
    let rps = (completed as f64 * 1000.0) / elapsed_ms as f64;
    println!(
        "stress_gate bench=stress_repository_metadata requests={completed} elapsed_ms={elapsed_ms} rps={rps:.2}"
    );

    server.shutdown().await;
}
