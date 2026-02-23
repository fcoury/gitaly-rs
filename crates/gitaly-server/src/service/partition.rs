use std::sync::Arc;

use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::partition_service_server::PartitionService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

#[derive(Debug, Clone)]
pub struct PartitionServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl PartitionServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn resolve_storage_root(&self, storage_name: &str) -> Result<std::path::PathBuf, Status> {
        if storage_name.trim().is_empty() {
            return Err(Status::invalid_argument("storage_name is required"));
        }

        self.dependencies
            .storage_paths
            .get(storage_name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("storage `{storage_name}` is not configured")))
    }

    fn partition_ids_for_storage(
        &self,
        storage_root: &std::path::Path,
    ) -> Result<Vec<String>, Status> {
        let wal_dir = storage_root.join("wal");
        if !wal_dir.exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&wal_dir).map_err(|err| {
            Status::internal(format!(
                "failed to enumerate partition directory `{}`: {err}",
                wal_dir.display()
            ))
        })?;

        let mut ids: Vec<String> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }

                if path.extension().and_then(|value| value.to_str()) != Some("wal") {
                    return None;
                }

                path.file_stem()
                    .and_then(|value| value.to_str())
                    .map(ToOwned::to_owned)
            })
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }
}

#[tonic::async_trait]
impl PartitionService for PartitionServiceImpl {
    async fn backup_partition(
        &self,
        request: Request<BackupPartitionRequest>,
    ) -> Result<Response<BackupPartitionResponse>, Status> {
        let request = request.into_inner();
        if request.partition_id.trim().is_empty() {
            return Err(Status::invalid_argument("partition_id is required"));
        }

        let storage_root = self.resolve_storage_root(&request.storage_name)?;
        let wal_file = storage_root
            .join("wal")
            .join(format!("{}.wal", request.partition_id));
        if !wal_file.exists() {
            return Err(Status::not_found(format!(
                "partition `{}` was not found in storage `{}`",
                request.partition_id, request.storage_name
            )));
        }

        Ok(Response::new(BackupPartitionResponse {}))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let request = request.into_inner();
        let storage_root = self.resolve_storage_root(&request.storage_name)?;
        let mut ids = self.partition_ids_for_storage(&storage_root)?;

        let mut page_token = String::new();
        let mut limit = None;
        if let Some(params) = request.pagination_params {
            page_token = params.page_token;
            if params.limit <= 0 {
                return Ok(Response::new(ListPartitionsResponse {
                    partitions: Vec::new(),
                    pagination_cursor: None,
                }));
            }
            limit = usize::try_from(params.limit).ok();
        }

        if !page_token.is_empty() {
            ids.retain(|id| id > &page_token);
        }

        let (selected, cursor) = if let Some(limit) = limit {
            if ids.len() > limit {
                let selected = ids[..limit].to_vec();
                let next_cursor = selected.last().cloned().unwrap_or_default();
                (
                    selected,
                    Some(PaginationCursor {
                        next_cursor,
                    }),
                )
            } else {
                (ids, None)
            }
        } else {
            (ids, None)
        };

        Ok(Response::new(ListPartitionsResponse {
            partitions: selected
                .into_iter()
                .map(|id| Partition { id })
                .collect(),
            pagination_cursor: cursor,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::Code;

    use gitaly_proto::gitaly::partition_service_client::PartitionServiceClient;
    use gitaly_proto::gitaly::partition_service_server::PartitionServiceServer;
    use gitaly_proto::gitaly::{BackupPartitionRequest, ListPartitionsRequest, PaginationParameter};

    use crate::dependencies::Dependencies;

    use super::PartitionServiceImpl;

    fn unique_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("gitaly-rs-{name}-{nanos}"))
    }

    async fn start_server(
        service: PartitionServiceImpl,
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
                .add_service(PartitionServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> PartitionServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match PartitionServiceClient::connect(endpoint.clone()).await {
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

    fn test_dependencies(storage_root: &std::path::Path) -> Arc<Dependencies> {
        let mut storage_paths = HashMap::new();
        storage_paths.insert("default".to_string(), storage_root.to_path_buf());
        Arc::new(Dependencies::default().with_storage_paths(storage_paths))
    }

    #[tokio::test]
    async fn list_partitions_reads_partition_ids_from_wal_directory() {
        let storage_root = unique_dir("partition-service-list");
        let wal_dir = storage_root.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("wal directory should be creatable");
        std::fs::write(wal_dir.join("p2.wal"), b"").expect("partition wal should be creatable");
        std::fs::write(wal_dir.join("p1.wal"), b"").expect("partition wal should be creatable");
        std::fs::write(wal_dir.join("ignore.txt"), b"").expect("other files should be creatable");

        let (endpoint, shutdown_tx, server_task) =
            start_server(PartitionServiceImpl::new(test_dependencies(&storage_root))).await;
        let mut client = connect_client(endpoint).await;

        let response = client
            .list_partitions(ListPartitionsRequest {
                storage_name: "default".to_string(),
                pagination_params: None,
            })
            .await
            .expect("list_partitions should succeed")
            .into_inner();
        let ids: Vec<String> = response.partitions.into_iter().map(|partition| partition.id).collect();
        assert_eq!(ids, vec!["p1".to_string(), "p2".to_string()]);

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn list_partitions_supports_simple_pagination() {
        let storage_root = unique_dir("partition-service-pagination");
        let wal_dir = storage_root.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("wal directory should be creatable");
        std::fs::write(wal_dir.join("p1.wal"), b"").expect("partition wal should be creatable");
        std::fs::write(wal_dir.join("p2.wal"), b"").expect("partition wal should be creatable");
        std::fs::write(wal_dir.join("p3.wal"), b"").expect("partition wal should be creatable");

        let (endpoint, shutdown_tx, server_task) =
            start_server(PartitionServiceImpl::new(test_dependencies(&storage_root))).await;
        let mut client = connect_client(endpoint).await;

        let first_page = client
            .list_partitions(ListPartitionsRequest {
                storage_name: "default".to_string(),
                pagination_params: Some(PaginationParameter {
                    page_token: String::new(),
                    limit: 2,
                }),
            })
            .await
            .expect("first page should succeed")
            .into_inner();
        let first_ids: Vec<String> = first_page
            .partitions
            .iter()
            .map(|partition| partition.id.clone())
            .collect();
        assert_eq!(first_ids, vec!["p1".to_string(), "p2".to_string()]);
        assert_eq!(
            first_page
                .pagination_cursor
                .as_ref()
                .expect("first page should have cursor")
                .next_cursor,
            "p2"
        );

        let second_page = client
            .list_partitions(ListPartitionsRequest {
                storage_name: "default".to_string(),
                pagination_params: Some(PaginationParameter {
                    page_token: "p2".to_string(),
                    limit: 2,
                }),
            })
            .await
            .expect("second page should succeed")
            .into_inner();
        let second_ids: Vec<String> = second_page
            .partitions
            .iter()
            .map(|partition| partition.id.clone())
            .collect();
        assert_eq!(second_ids, vec!["p3".to_string()]);
        assert!(second_page.pagination_cursor.is_none());

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }

    #[tokio::test]
    async fn backup_partition_validates_existence() {
        let storage_root = unique_dir("partition-service-backup");
        let wal_dir = storage_root.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("wal directory should be creatable");
        std::fs::write(wal_dir.join("p1.wal"), b"").expect("partition wal should be creatable");

        let (endpoint, shutdown_tx, server_task) =
            start_server(PartitionServiceImpl::new(test_dependencies(&storage_root))).await;
        let mut client = connect_client(endpoint).await;

        client
            .backup_partition(BackupPartitionRequest {
                storage_name: "default".to_string(),
                partition_id: "p1".to_string(),
            })
            .await
            .expect("existing partition should be accepted");

        let missing = client
            .backup_partition(BackupPartitionRequest {
                storage_name: "default".to_string(),
                partition_id: "p9".to_string(),
            })
            .await
            .expect_err("missing partition should fail");
        assert_eq!(missing.code(), Code::NotFound);

        shutdown_server(shutdown_tx, server_task).await;
        if storage_root.exists() {
            std::fs::remove_dir_all(storage_root).expect("storage root should be removable");
        }
    }
}
