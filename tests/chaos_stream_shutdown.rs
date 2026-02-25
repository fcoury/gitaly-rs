mod support;

use std::time::Duration;

use gitaly_proto::gitaly::ServerInfoRequest;
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_breaks_follow_up_request_and_reconnect() {
    let server = support::start_test_server("chaos-stream-shutdown").await;
    let endpoint = server.endpoint.clone();
    let mut client = support::connect_server_client(&endpoint).await;

    let first = client
        .server_info(ServerInfoRequest {})
        .await
        .expect("initial server_info should succeed")
        .into_inner();
    assert!(
        !first.server_version.trim().is_empty(),
        "initial response should include server version"
    );

    server.shutdown().await;

    let follow_up = client
        .server_info(ServerInfoRequest {})
        .await
        .expect_err("server_info should fail after shutdown");
    assert!(
        matches!(follow_up.code(), Code::Unavailable | Code::Unknown),
        "shutdown should return transport-like error, got {:?}",
        follow_up.code()
    );

    let reconnect =
        support::try_connect_server_client(&endpoint, 5, Duration::from_millis(20)).await;
    assert!(
        reconnect.is_err(),
        "new connections should fail after shutdown"
    );
}
