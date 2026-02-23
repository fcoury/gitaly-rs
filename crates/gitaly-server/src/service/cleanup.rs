use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::cleanup_service_server::CleanupService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct CleanupServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl CleanupServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }

    fn ensure_storage_configured(&self, storage_name: &str) -> Result<(), Status> {
        if storage_name.trim().is_empty() {
            return Err(Status::invalid_argument(
                "repository.storage_name is required",
            ));
        }

        if self.dependencies.storage_paths.contains_key(storage_name) {
            return Ok(());
        }

        Err(Status::not_found(format!(
            "storage `{storage_name}` is not configured"
        )))
    }
}

fn parse_object_map_entries(
    payload: &[u8],
) -> Result<Vec<apply_bfg_object_map_stream_response::Entry>, Status> {
    let mut entries = Vec::new();
    for (index, raw_line) in String::from_utf8_lossy(payload).lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let old_oid = parts.next().unwrap_or_default();
        let new_oid = parts.next().unwrap_or_default();
        if old_oid.is_empty() || new_oid.is_empty() || parts.next().is_some() {
            return Err(Status::invalid_argument(format!(
                "invalid object-map entry at line {}",
                index + 1
            )));
        }

        if !old_oid.chars().all(|char| char.is_ascii_hexdigit())
            || !new_oid.chars().all(|char| char.is_ascii_hexdigit())
        {
            return Err(Status::invalid_argument(format!(
                "object-map line {} contains non-hex object id",
                index + 1
            )));
        }

        entries.push(apply_bfg_object_map_stream_response::Entry {
            r#type: ObjectType::Unknown as i32,
            old_oid: old_oid.to_string(),
            new_oid: new_oid.to_string(),
        });
    }

    Ok(entries)
}

#[tonic::async_trait]
impl CleanupService for CleanupServiceImpl {
    type ApplyBfgObjectMapStreamStream = ServiceStream<ApplyBfgObjectMapStreamResponse>;

    async fn apply_bfg_object_map_stream(
        &self,
        request: Request<tonic::Streaming<ApplyBfgObjectMapStreamRequest>>,
    ) -> Result<Response<Self::ApplyBfgObjectMapStreamStream>, Status> {
        let mut stream = request.into_inner();
        let mut saw_repository = false;
        let mut object_map_payload = Vec::new();

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !saw_repository {
                let repository = message
                    .repository
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("first message must include repository"))?;
                self.ensure_storage_configured(&repository.storage_name)?;
                saw_repository = true;
            }

            if !message.object_map.is_empty() {
                object_map_payload.extend_from_slice(&message.object_map);
            }
        }

        if !saw_repository {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        let entries = parse_object_map_entries(&object_map_payload)?;
        let response = ApplyBfgObjectMapStreamResponse { entries };
        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(response)]))))
    }

    async fn rewrite_history(
        &self,
        request: Request<tonic::Streaming<RewriteHistoryRequest>>,
    ) -> Result<Response<RewriteHistoryResponse>, Status> {
        let mut stream = request.into_inner();
        let mut saw_repository = false;

        while let Some(message) = stream
            .message()
            .await
            .map_err(|err| Status::invalid_argument(format!("invalid request stream: {err}")))?
        {
            if !saw_repository {
                let repository = message
                    .repository
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("first message must include repository"))?;
                self.ensure_storage_configured(&repository.storage_name)?;
                saw_repository = true;
            }
        }

        if !saw_repository {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(Response::new(RewriteHistoryResponse {}))
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
    use tonic::Code;

    use gitaly_proto::gitaly::cleanup_service_client::CleanupServiceClient;
    use gitaly_proto::gitaly::cleanup_service_server::CleanupServiceServer;
    use gitaly_proto::gitaly::{
        ApplyBfgObjectMapStreamRequest, ObjectType, Repository, RewriteHistoryRequest,
    };

    use crate::dependencies::Dependencies;

    use super::CleanupServiceImpl;

    async fn start_server(
        service: CleanupServiceImpl,
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
                .add_service(CleanupServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> CleanupServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match CleanupServiceClient::connect(endpoint.clone()).await {
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
    async fn apply_bfg_object_map_stream_parses_object_map_entries() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(CleanupServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let stream = tokio_stream::iter(vec![ApplyBfgObjectMapStreamRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            object_map: b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\ncccccccccccccccccccccccccccccccccccccccc dddddddddddddddddddddddddddddddddddddddd\n".to_vec(),
        }]);
        let mut response_stream = client
            .apply_bfg_object_map_stream(stream)
            .await
            .expect("apply_bfg_object_map_stream should succeed")
            .into_inner();

        let first = response_stream
            .message()
            .await
            .expect("stream read should succeed")
            .expect("first response should exist");
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.entries[0].r#type, ObjectType::Unknown as i32);
        assert_eq!(first.entries[0].old_oid, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(first.entries[0].new_oid, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let end = response_stream
            .message()
            .await
            .expect("stream read should succeed");
        assert!(end.is_none());

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn rewrite_history_requires_repository_on_first_message() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(CleanupServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let stream = tokio_stream::iter(vec![RewriteHistoryRequest {
            repository: None,
            blobs: vec!["deadbeef".to_string()],
            redactions: Vec::new(),
        }]);
        let status = client
            .rewrite_history(stream)
            .await
            .expect_err("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn rewrite_history_accepts_valid_request_stream() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(CleanupServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let stream = tokio_stream::iter(vec![RewriteHistoryRequest {
            repository: Some(Repository {
                storage_name: "default".to_string(),
                relative_path: "group/project.git".to_string(),
                ..Repository::default()
            }),
            blobs: vec!["deadbeef".to_string()],
            redactions: vec![b"secret".to_vec()],
        }]);
        client
            .rewrite_history(stream)
            .await
            .expect("rewrite_history should succeed");

        shutdown_server(shutdown_tx, server_task).await;
    }
}
