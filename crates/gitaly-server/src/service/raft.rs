use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_cluster::ClusterStateManager;
use gitaly_proto::gitaly::raft_service_server::RaftService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct RaftServiceImpl {
    dependencies: Arc<Dependencies>,
    cluster_state: Arc<ClusterStateManager>,
}

impl RaftServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self {
            dependencies,
            cluster_state: Arc::new(ClusterStateManager::new()),
        }
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
            self.cluster_state.record_message(&message);
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
        let mut header = None;
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
                    header = Some(raft_msg);
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

        let snapshot_record = self
            .cluster_state
            .record_snapshot(&header.expect("header should exist"), snapshot_size);

        Ok(Response::new(RaftSnapshotMessageResponse {
            destination: snapshot_record.destination,
            snapshot_size: snapshot_record.snapshot_size,
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

        self.cluster_state.process_join_cluster(&request);

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

        let responses = self.cluster_state.get_partitions(&request);

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

        Ok(Response::new(
            self.cluster_state.get_cluster_info(&request.cluster_id),
        ))
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
        raft_snapshot_message_request, replica_id, GetPartitionsRequest, JoinClusterRequest,
        RaftClusterInfoRequest, RaftMessageRequest, RaftPartitionKey, RaftSnapshotMessageRequest,
        ReplicaId,
    };
    use gitaly_proto::raftpb::{Message, Snapshot, SnapshotMetadata};

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
    async fn raft_state_reflects_message_join_and_snapshot_flows() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(RaftServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let message_stream = tokio_stream::iter(vec![RaftMessageRequest {
            cluster_id: "cluster-a".to_string(),
            replica_id: Some(ReplicaId {
                partition_key: Some(RaftPartitionKey {
                    value: "partition-a".to_string(),
                }),
                member_id: 1,
                storage_name: "default".to_string(),
                metadata: Some(replica_id::Metadata {
                    address: "127.0.0.1:2301".to_string(),
                }),
                r#type: replica_id::ReplicaType::Voter as i32,
            }),
            message: Some(Message {
                from: Some(1),
                term: Some(4),
                index: Some(12),
                ..Message::default()
            }),
            ..RaftMessageRequest::default()
        }]);
        client
            .send_message(message_stream)
            .await
            .expect("send_message should succeed");

        client
            .send_snapshot(tokio_stream::iter(vec![
                RaftSnapshotMessageRequest {
                    raft_snapshot_payload: Some(
                        raft_snapshot_message_request::RaftSnapshotPayload::RaftMsg(
                            RaftMessageRequest {
                                cluster_id: "cluster-a".to_string(),
                                replica_id: Some(ReplicaId {
                                    partition_key: Some(RaftPartitionKey {
                                        value: "partition-a".to_string(),
                                    }),
                                    member_id: 1,
                                    storage_name: "default".to_string(),
                                    metadata: Some(replica_id::Metadata {
                                        address: "127.0.0.1:2301".to_string(),
                                    }),
                                    r#type: replica_id::ReplicaType::Voter as i32,
                                }),
                                message: Some(Message {
                                    from: Some(1),
                                    term: Some(5),
                                    index: Some(20),
                                    snapshot: Some(Snapshot {
                                        metadata: Some(SnapshotMetadata {
                                            conf_state: None,
                                            index: Some(20),
                                            term: Some(5),
                                        }),
                                        data: None,
                                    }),
                                    ..Message::default()
                                }),
                            },
                        ),
                    ),
                },
                RaftSnapshotMessageRequest {
                    raft_snapshot_payload: Some(
                        raft_snapshot_message_request::RaftSnapshotPayload::Chunk(vec![1; 8]),
                    ),
                },
                RaftSnapshotMessageRequest {
                    raft_snapshot_payload: Some(
                        raft_snapshot_message_request::RaftSnapshotPayload::Chunk(vec![2; 8]),
                    ),
                },
            ]))
            .await
            .expect("send_snapshot should succeed");

        client
            .join_cluster(JoinClusterRequest {
                partition_key: Some(RaftPartitionKey {
                    value: "partition-a".to_string(),
                }),
                leader_id: 1,
                member_id: 2,
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                replicas: vec![
                    ReplicaId {
                        partition_key: Some(RaftPartitionKey {
                            value: "partition-a".to_string(),
                        }),
                        member_id: 1,
                        storage_name: "default".to_string(),
                        metadata: Some(replica_id::Metadata {
                            address: "127.0.0.1:2301".to_string(),
                        }),
                        r#type: replica_id::ReplicaType::Voter as i32,
                    },
                    ReplicaId {
                        partition_key: Some(RaftPartitionKey {
                            value: "partition-a".to_string(),
                        }),
                        member_id: 2,
                        storage_name: "secondary".to_string(),
                        metadata: Some(replica_id::Metadata {
                            address: "127.0.0.1:2302".to_string(),
                        }),
                        r#type: replica_id::ReplicaType::Learner as i32,
                    },
                ],
            })
            .await
            .expect("join_cluster should succeed");

        let mut partitions_stream = client
            .get_partitions(GetPartitionsRequest {
                cluster_id: "cluster-a".to_string(),
                partition_key: Some(RaftPartitionKey {
                    value: "partition-a".to_string(),
                }),
                relative_path: "group/project.git".to_string(),
                include_replica_details: true,
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
        assert_eq!(first.leader_id, 1);
        assert_eq!(first.term, 5);
        assert_eq!(first.index, 20);
        assert_eq!(first.replicas.len(), 2);
        assert_eq!(first.relative_paths, vec!["group/project.git".to_string()]);

        let end = partitions_stream.next().await;
        assert!(end.is_none());

        let cluster_info = client
            .get_cluster_info(RaftClusterInfoRequest {
                cluster_id: "cluster-a".to_string(),
            })
            .await
            .expect("get_cluster_info should succeed")
            .into_inner();
        assert_eq!(cluster_info.cluster_id, "cluster-a");
        let statistics = cluster_info
            .statistics
            .expect("statistics should be available");
        assert_eq!(statistics.total_partitions, 1);
        assert_eq!(statistics.healthy_partitions, 1);
        assert_eq!(statistics.total_replicas, 2);
        assert_eq!(statistics.healthy_replicas, 2);
        assert_eq!(
            statistics
                .storage_stats
                .get("default")
                .expect("default storage stats should exist")
                .leader_count,
            1
        );
        assert_eq!(
            statistics
                .storage_stats
                .get("secondary")
                .expect("secondary storage stats should exist")
                .replica_count,
            1
        );

        shutdown_server(shutdown_tx, server_task).await;
    }
}
