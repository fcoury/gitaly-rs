use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use gitaly_proto::gitaly::analysis_service_server::AnalysisService;
use gitaly_proto::gitaly::*;

use crate::dependencies::Dependencies;

type ServiceStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct AnalysisServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl AnalysisServiceImpl {
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

fn generated_from_path(path: &[u8]) -> bool {
    let path = String::from_utf8_lossy(path).to_ascii_lowercase();
    if path.ends_with(".min.js")
        || path.ends_with(".min.css")
        || path.ends_with(".map")
        || path.ends_with(".pb.go")
        || path.ends_with(".generated.rs")
        || path.ends_with(".generated.ts")
    {
        return true;
    }

    path.contains("/generated/")
        || path.contains("/dist/")
        || path.contains("/vendor/")
        || path.starts_with("vendor/")
        || path.contains("generated.")
}

#[tonic::async_trait]
impl AnalysisService for AnalysisServiceImpl {
    type CheckBlobsGeneratedStream = ServiceStream<CheckBlobsGeneratedResponse>;

    async fn check_blobs_generated(
        &self,
        request: Request<tonic::Streaming<CheckBlobsGeneratedRequest>>,
    ) -> Result<Response<Self::CheckBlobsGeneratedStream>, Status> {
        let mut stream = request.into_inner();
        let mut saw_repository = false;
        let mut responses = Vec::new();

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

            if message.blobs.is_empty() {
                return Err(Status::invalid_argument(
                    "each request must contain at least one blob entry",
                ));
            }

            let blobs = message
                .blobs
                .into_iter()
                .map(|blob| check_blobs_generated_response::Blob {
                    revision: blob.revision,
                    generated: generated_from_path(&blob.path),
                })
                .collect();

            responses.push(CheckBlobsGeneratedResponse { blobs });
        }

        if !saw_repository {
            return Err(Status::invalid_argument(
                "request stream must contain at least one message",
            ));
        }

        Ok(Response::new(Box::pin(tokio_stream::iter(
            responses.into_iter().map(Ok),
        ))))
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

    use gitaly_proto::gitaly::analysis_service_client::AnalysisServiceClient;
    use gitaly_proto::gitaly::analysis_service_server::AnalysisServiceServer;
    use gitaly_proto::gitaly::{check_blobs_generated_request, CheckBlobsGeneratedRequest, Repository};

    use crate::dependencies::Dependencies;

    use super::AnalysisServiceImpl;

    async fn start_server(
        service: AnalysisServiceImpl,
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
                .add_service(AnalysisServiceServer::new(service))
                .serve_with_shutdown(server_addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });

        (format!("http://{server_addr}"), shutdown_tx, server_task)
    }

    async fn connect_client(endpoint: String) -> AnalysisServiceClient<tonic::transport::Channel> {
        for _ in 0..50 {
            match AnalysisServiceClient::connect(endpoint.clone()).await {
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
    async fn check_blobs_generated_requires_repository_in_first_message() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(AnalysisServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![CheckBlobsGeneratedRequest {
            repository: None,
            blobs: vec![check_blobs_generated_request::Blob {
                revision: b"HEAD:README.md".to_vec(),
                path: b"README.md".to_vec(),
            }],
        }]);
        let status = client
            .check_blobs_generated(request_stream)
            .await
            .expect_err("missing repository should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        shutdown_server(shutdown_tx, server_task).await;
    }

    #[tokio::test]
    async fn check_blobs_generated_returns_results_for_each_stream_message() {
        let (endpoint, shutdown_tx, server_task) =
            start_server(AnalysisServiceImpl::new(test_dependencies())).await;
        let mut client = connect_client(endpoint).await;

        let request_stream = tokio_stream::iter(vec![
            CheckBlobsGeneratedRequest {
                repository: Some(Repository {
                    storage_name: "default".to_string(),
                    relative_path: "group/project.git".to_string(),
                    ..Repository::default()
                }),
                blobs: vec![
                    check_blobs_generated_request::Blob {
                        revision: b"HEAD:app.min.js".to_vec(),
                        path: b"public/app.min.js".to_vec(),
                    },
                    check_blobs_generated_request::Blob {
                        revision: b"HEAD:src/main.rs".to_vec(),
                        path: b"src/main.rs".to_vec(),
                    },
                ],
            },
            CheckBlobsGeneratedRequest {
                repository: None,
                blobs: vec![check_blobs_generated_request::Blob {
                    revision: b"HEAD:vendor/lib.js".to_vec(),
                    path: b"vendor/lib.js".to_vec(),
                }],
            },
        ]);

        let mut response_stream = client
            .check_blobs_generated(request_stream)
            .await
            .expect("check_blobs_generated should succeed")
            .into_inner();

        let first = response_stream
            .message()
            .await
            .expect("stream read should succeed")
            .expect("first response should exist");
        assert_eq!(first.blobs.len(), 2);
        assert!(first.blobs[0].generated);
        assert!(!first.blobs[1].generated);

        let second = response_stream
            .message()
            .await
            .expect("stream read should succeed")
            .expect("second response should exist");
        assert_eq!(second.blobs.len(), 1);
        assert!(second.blobs[0].generated);

        let end = response_stream
            .message()
            .await
            .expect("stream read should succeed");
        assert!(end.is_none());

        shutdown_server(shutdown_tx, server_task).await;
    }
}
