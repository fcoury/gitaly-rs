mod support;

use gitaly_proto::gitaly::ServerInfoRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_info_smoke_returns_versions() {
    let server = support::start_test_server("integration-server-info").await;
    let mut client = support::connect_server_client(&server.endpoint).await;

    let response = client
        .server_info(ServerInfoRequest {})
        .await
        .expect("server_info should succeed")
        .into_inner();

    assert!(
        !response.server_version.trim().is_empty(),
        "server_version should not be empty"
    );
    assert!(
        response.server_version == env!("CARGO_PKG_VERSION")
            || !response.server_version.trim().is_empty(),
        "server_version should be either default or a non-empty override"
    );
    assert!(
        response.git_version == "unknown" || response.git_version.starts_with("git version "),
        "git_version should be unknown by default or a git --version string"
    );

    server.shutdown().await;
}
