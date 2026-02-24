use gitaly_proto::gitaly::server_service_client::ServerServiceClient;
use gitaly_proto::gitaly::{
    DiskStatisticsRequest, ReadinessCheckRequest, ServerInfoRequest, ServerSignatureRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ServerServiceClient::connect("http://127.0.0.1:2305").await?;

    let server_info = client.server_info(ServerInfoRequest {}).await?.into_inner();
    println!("--- ServerInfo");
    println!("server_version={}", server_info.server_version);
    println!("git_version={}", server_info.git_version);
    println!("storage_statuses={}", server_info.storage_statuses.len());

    let disk_stats = client
        .disk_statistics(DiskStatisticsRequest {})
        .await?
        .into_inner();
    println!("--- DiskStatistics");
    println!("storage_statuses={}", disk_stats.storage_statuses.len());

    let readiness = client
        .readiness_check(ReadinessCheckRequest { timeout: None })
        .await?
        .into_inner();
    println!("--- ReadinessCheck");
    println!("has_result={}", readiness.result.is_some());

    let signature = client
        .server_signature(ServerSignatureRequest {})
        .await?
        .into_inner();
    println!("--- ServerSignature");
    println!("public_key_len={}", signature.public_key.len());

    Ok(())
}
