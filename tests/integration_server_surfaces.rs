mod support;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use gitaly_proto::gitaly::{
    readiness_check_response, DiskStatisticsRequest, ReadinessCheckRequest, ServerSignatureRequest,
};
use gitaly_server::{Dependencies, StorageStatus};

fn unique_storage_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "gitaly-rs-integration-{test_name}-{}-{nanos}",
        std::process::id()
    ))
}

fn build_dependencies(storage_root: PathBuf) -> Dependencies {
    std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");

    let mut storage_paths = HashMap::new();
    storage_paths.insert("default".to_string(), storage_root);

    Dependencies::default()
        .with_storage_paths(storage_paths)
        .with_storage_statuses(vec![StorageStatus {
            storage_name: "default".to_string(),
            readable: true,
            writeable: true,
            fs_type: "tmpfs".to_string(),
            filesystem_id: "fs-test".to_string(),
            replication_factor: 2,
            available: 1234,
            used: 567,
        }])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_statistics_reports_dependency_storage_values() {
    let storage_root = unique_storage_root("disk-statistics");
    let dependencies = Arc::new(build_dependencies(storage_root.clone()));
    let server = support::start_test_server_with_dependencies(
        "integration-server-disk-statistics",
        Arc::clone(&dependencies),
    )
    .await;
    let mut client = support::connect_server_client(&server.endpoint).await;

    let response = client
        .disk_statistics(DiskStatisticsRequest {})
        .await
        .expect("disk_statistics should succeed")
        .into_inner();
    assert_eq!(response.storage_statuses.len(), 1);
    let status = &response.storage_statuses[0];
    assert_eq!(status.storage_name, "default");
    assert_eq!(status.available, 1234);
    assert_eq!(status.used, 567);

    server.shutdown().await;
    let _ = std::fs::remove_dir_all(storage_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_check_reports_failure_when_server_is_not_ready() {
    let storage_root = unique_storage_root("readiness-failure");
    let dependencies = Arc::new(build_dependencies(storage_root.clone()).with_ready(false));
    let server = support::start_test_server_with_dependencies(
        "integration-server-readiness-failure",
        dependencies,
    )
    .await;
    let mut client = support::connect_server_client(&server.endpoint).await;

    let response = client
        .readiness_check(ReadinessCheckRequest { timeout: None })
        .await
        .expect("readiness_check should succeed")
        .into_inner();
    assert!(matches!(
        response.result,
        Some(readiness_check_response::Result::FailureResponse(_))
    ));

    server.shutdown().await;
    let _ = std::fs::remove_dir_all(storage_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_signature_returns_configured_public_key() {
    let storage_root = unique_storage_root("server-signature");
    let dependencies = Arc::new(
        build_dependencies(storage_root.clone())
            .with_server_signature_public_key(b"ssh-ed25519 AAAA-test-signature".to_vec()),
    );
    let server = support::start_test_server_with_dependencies(
        "integration-server-signature",
        dependencies,
    )
    .await;
    let mut client = support::connect_server_client(&server.endpoint).await;

    let response = client
        .server_signature(ServerSignatureRequest {})
        .await
        .expect("server_signature should succeed")
        .into_inner();
    assert_eq!(response.public_key, b"ssh-ed25519 AAAA-test-signature");

    server.shutdown().await;
    let _ = std::fs::remove_dir_all(storage_root);
}
