use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::raft_service_server::RaftService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct RaftServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl RaftServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn ensure_storage_configured(&self, storage_name: &str) -> Result<(), Status> {
        if storage_name.trim().is_empty() {
            return Err(Status::invalid_argument("storage_name is required"));
        }

        if self.dependencies.storage_paths.contains_key(storage_name) {
            return Ok(());
        }

        Err(Status::not_found(format!(
            "storage `{storage_name}` is not configured"
        )))
    }
}

fn stream_from_values<T>(values: Vec<T>) -> Response<ServiceStream<T>>
where
    T: Send + 'static,
{
    Response::new(Box::pin(tokio_stream::iter(values.into_iter().map(Ok))))
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    type GetPartitionsStream = ServiceStream<GetPartitionsResponse>;

    async fn send_message(
        &self,
        request: Request<tonic::Streaming<RaftMessageRequest>>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        let mut stream = request.into_inner();
        let mut saw_message = false;
        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            saw_message = true;
            if message.cluster_id.trim().is_empty() {
                return Err(Status::invalid_argument("cluster_id is required"));
            }
        }

        if !saw_message {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(Response::new(RaftMessageResponse {}))
    }

    async fn send_snapshot(
        &self,
        request: Request<tonic::Streaming<RaftSnapshotMessageRequest>>,
    ) -> Result<Response<RaftSnapshotMessageResponse>, Status> {
        let mut stream = request.into_inner();
        let mut saw_header = false;
        let mut snapshot_size = 0_u64;

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            match message.raft_snapshot_payload {
                Some(raft_snapshot_message_request::RaftSnapshotPayload::RaftMsg(raft_msg)) => {
                    if raft_msg.cluster_id.trim().is_empty() {
                        return Err(Status::invalid_argument("cluster_id is required"));
                    }
                    saw_header = true;
                }
                Some(raft_snapshot_message_request::RaftSnapshotPayload::Chunk(chunk)) => {
                    snapshot_size = snapshot_size.saturating_add(chunk.len() as u64);
                }
                None => {}
            }
        }

        if !saw_header {
            return Err(Status::invalid_argument(
                "first snapshot stream message must include raft metadata",
            ));
        }

        Ok(Response::new(RaftSnapshotMessageResponse {
            destination: "in-memory".to_string(),
            snapshot_size,
        }))
    }

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        let request = request.into_inner();
        self.ensure_storage_configured(&request.storage_name)?;
        if request.relative_path.trim().is_empty() {
            return Err(Status::invalid_argument("relative_path is required"));
        }
        if request.partition_key.is_none() {
            return Err(Status::invalid_argument("partition_key is required"));
        }

        Ok(Response::new(JoinClusterResponse {}))
    }

    async fn get_partitions(
        &self,
        request: Request<GetPartitionsRequest>,
    ) -> Result<Response<Self::GetPartitionsStream>, Status> {
        let request = request.into_inner();
        if request.cluster_id.trim().is_empty() {
            return Err(Status::invalid_argument("cluster_id is required"));
        }

        let mut responses = Vec::new();
        if let Some(partition_key) = request.partition_key {
            responses.push(GetPartitionsResponse {
                cluster_id: request.cluster_id.clone(),
                partition_key: Some(partition_key),
                replicas: Vec::new(),
                leader_id: 0,
                term: 0,
                index: 0,
                relative_path: request.relative_path.clone(),
                relative_paths: if request.include_relative_paths && !request.relative_path.is_empty()
                {
                    vec![request.relative_path]
                } else {
                    Vec::new()
                },
            });
        }

        Ok(stream_from_values(responses))
    }

    async fn get_cluster_info(
        &self,
        request: Request<RaftClusterInfoRequest>,
    ) -> Result<Response<RaftClusterInfoResponse>, Status> {
        let request = request.into_inner();
        if request.cluster_id.trim().is_empty() {
            return Err(Status::invalid_argument("cluster_id is required"));
        }

        Ok(Response::new(RaftClusterInfoResponse {
            cluster_id: request.cluster_id,
            statistics: Some(ClusterStatistics {
                total_partitions: 0,
                healthy_partitions: 0,
                total_replicas: 0,
                healthy_replicas: 0,
                storage_stats: std::collections::HashMap::new(),
            }),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;

    use gitaly_proto::gitaly::raft_service_client::RaftServiceClient;
    use gitaly_proto::gitaly::raft_service_server::RaftServiceServer;
    use gitaly_proto::gitaly::{
        replica_id, GetPartitionsRequest, JoinClusterRequest, RaftClusterInfoRequest,
        RaftMessageRequest, RaftPartitionKey, ReplicaId,
    };

    use crate::dependencies::Dependencies;

    use super::RaftServiceImpl;

    async fn start_server(
        service: RaftServiceImpl,
    ) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let server_addr = listener
            .local_addr()
            .expect("ephemeral listener should provide local address");
        drop(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> RaftServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match RaftServiceClient::connect(endpoint.clone()).await {
                Ok(client) => return client,
                Err(_) => sleep(Duration::from_millis(10)).await,
            }
        }

        panic!("server did not become reachable at {endpoint}");
    }

    async fn shutdown_server(shutdown_tx: oneshot::Sender<()>, server_task: JoinHandle<()>) {
        let _ = shutdown_tx.send(());
        let join_result = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server should stop within timeout");
        join_result.expect("server task should not panic");
    }

    fn test_dependencies() -> Arc<Dependencies> {
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), std::env::temp_dir());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn send_message_and_get_partitions_return_structured_responses() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(RaftServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let message_stream = tokio_stream::iter(vec![RaftMessageRequest {
            cluster_id: "cluster-a".to_string(),
            ..RaftMessageRequest::default()
        }]);
        client
            .send_message(message_stream)
            .await
            .expect("send_message should succeed");

        let mut partitions_stream = client
            .get_partitions(GetPartitionsRequest {
                cluster_id: "cluster-a".to_string(),
                partition_key: Some(RaftPartitionKey {
                    value: "partition-a".to_string(),
                }),
                relative_path: "group/project.git".to_string(),
                include_relative_paths: true,
                ..GetPartitionsRequest::default()
            })
            .await
            .expect("get_partitions should succeed")
            .into_inner();
        let first = partitions_stream
            .next()
            .await
            .expect("partition response should exist")
            .expect("partition response should succeed");
        assert_eq!(first.cluster_id, "cluster-a");
        assert_eq!(first.relative_paths, vec!["group/project.git".to_string()]);

        let end = partitions_stream.next().await;
        assert!(end.is_none());

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn join_cluster_and_cluster_info_validate_inputs() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(RaftServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        client
            .join_cluster(JoinClusterRequest {
                partition_key: Some(RaftPartitionKey {
                    value: "partition-a".to_string(),
                }),
                leader_id: 1,
                member_id: 2,
                storage_name: "default".to_string(),
                relative_path: "partitions/p1".to_string(),
                replicas: vec![ReplicaId {
                    partition_key: Some(RaftPartitionKey {
                        value: "partition-a".to_string(),
                    }),
                    member_id: 2,
                    storage_name: "default".to_string(),
                    metadata: Some(replica_id::Metadata {
                        address: "127.0.0.1:2305".to_string(),
                    }),
                    r#type: 0,
                }],
            })
            .await
            .expect("join_cluster should succeed");

        let cluster_info = client
            .get_cluster_info(RaftClusterInfoRequest {
                cluster_id: "cluster-a".to_string(),
            })
            .await
            .expect("get_cluster_info should succeed")
            .into_inner();
        assert_eq!(cluster_info.cluster_id, "cluster-a");
        assert!(cluster_info.statistics.is_some());

        shutdown_server(shutdown_tx, server_task).await;
    }
}
