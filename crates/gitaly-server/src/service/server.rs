use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::dependencies::Dependencies;
use gitaly_proto::gitaly::server_service_server::ServerService;
use gitaly_proto::gitaly::{
    disk_statistics_response, readiness_check_response, server_info_response,
    DiskStatisticsRequest, DiskStatisticsResponse, ReadinessCheckRequest, ReadinessCheckResponse,
    ServerInfoRequest, ServerInfoResponse, ServerSignatureRequest, ServerSignatureResponse,
};

#[derive(Debug, Clone)]
pub struct ServerServiceImpl {
    dependencies: Arc<Dependencies>,
}

impl ServerServiceImpl {
    #[must_use]
    pub fn new(dependencies: Arc<Dependencies>) -> Self {
        Self { dependencies }
    }
}

impl Default for ServerServiceImpl {
    fn default() -> Self {
        Self::new(Arc::new(Dependencies::default()))
    }
}

#[tonic::async_trait]
impl ServerService for ServerServiceImpl {
    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        let dependencies = &self.dependencies;
        let response = ServerInfoResponse {
            server_version: dependencies.server_version.clone(),
            git_version: dependencies.git_version.clone(),
            storage_statuses: dependencies
                .storage_statuses
                .iter()
                .map(|status| server_info_response::StorageStatus {
                    storage_name: status.storage_name.clone(),
                    readable: status.readable,
                    writeable: status.writeable,
                    fs_type: status.fs_type.clone(),
                    filesystem_id: status.filesystem_id.clone(),
                    replication_factor: status.replication_factor,
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn disk_statistics(
        &self,
        _request: Request<DiskStatisticsRequest>,
    ) -> Result<Response<DiskStatisticsResponse>, Status> {
        let dependencies = &self.dependencies;
        let response = DiskStatisticsResponse {
            storage_statuses: dependencies
                .storage_statuses
                .iter()
                .map(|status| disk_statistics_response::StorageStatus {
                    storage_name: status.storage_name.clone(),
                    available: status.available,
                    used: status.used,
                })
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn readiness_check(
        &self,
        _request: Request<ReadinessCheckRequest>,
    ) -> Result<Response<ReadinessCheckResponse>, Status> {
        let response = if self.dependencies.ready {
            ReadinessCheckResponse {
                result: Some(readiness_check_response::Result::OkResponse(
                    readiness_check_response::Ok {},
                )),
            }
        } else {
            ReadinessCheckResponse {
                result: Some(readiness_check_response::Result::FailureResponse(
                    readiness_check_response::Failure {
                        failed_checks: vec![readiness_check_response::failure::Response {
                            name: "readiness".to_string(),
                            error_message: "server is not ready".to_string(),
                        }],
                    },
                )),
            }
        };

        Ok(Response::new(response))
    }

    async fn server_signature(
        &self,
        _request: Request<ServerSignatureRequest>,
    ) -> Result<Response<ServerSignatureResponse>, Status> {
        let response = ServerSignatureResponse {
            public_key: self.dependencies.server_signature_public_key.clone(),
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::Request;

    use gitaly_proto::gitaly::server_service_server::ServerService;
    use gitaly_proto::gitaly::{
        readiness_check_response, DiskStatisticsRequest, ReadinessCheckRequest, ServerInfoRequest,
        ServerSignatureRequest,
    };

    use crate::dependencies::{Dependencies, StorageStatus};

    use super::ServerServiceImpl;

    fn test_dependencies(ready: bool) -> Arc<Dependencies> {
        Arc::new(Dependencies {
            server_version: "gitaly-rs-test".to_string(),
            git_version: "git version 2.47.0".to_string(),
            storage_statuses: vec![StorageStatus {
                storage_name: "test-storage".to_string(),
                readable: false,
                writeable: true,
                fs_type: "ext4".to_string(),
                filesystem_id: "fs-test-1".to_string(),
                replication_factor: 3,
                available: 7,
                used: 11,
            }],
            server_signature_public_key: b"ssh-ed25519 AAAA-test-key".to_vec(),
            ready,
        })
    }

    #[tokio::test]
    async fn server_info_returns_bootstrap_values() {
        let dependencies = test_dependencies(true);
        let response = ServerServiceImpl::new(Arc::clone(&dependencies))
            .server_info(Request::new(ServerInfoRequest {}))
            .await
            .expect("server_info should succeed")
            .into_inner();

        assert_eq!(
            response.server_version,
            dependencies.server_version.as_str()
        );
        assert_eq!(response.git_version, dependencies.git_version.as_str());
        assert_eq!(response.storage_statuses.len(), 1);
        let storage = &response.storage_statuses[0];
        let expected_storage = &dependencies.storage_statuses[0];
        assert_eq!(storage.storage_name, expected_storage.storage_name);
        assert_eq!(storage.readable, expected_storage.readable);
        assert_eq!(storage.writeable, expected_storage.writeable);
        assert_eq!(storage.fs_type, expected_storage.fs_type);
        assert_eq!(storage.filesystem_id, expected_storage.filesystem_id);
        assert_eq!(
            storage.replication_factor,
            expected_storage.replication_factor
        );
    }

    #[tokio::test]
    async fn disk_statistics_returns_storage_stats_from_dependencies() {
        let dependencies = test_dependencies(true);
        let response = ServerServiceImpl::new(Arc::clone(&dependencies))
            .disk_statistics(Request::new(DiskStatisticsRequest {}))
            .await
            .expect("disk_statistics should succeed")
            .into_inner();

        assert_eq!(response.storage_statuses.len(), 1);
        let storage = &response.storage_statuses[0];
        let expected_storage = &dependencies.storage_statuses[0];
        assert_eq!(storage.storage_name, expected_storage.storage_name);
        assert_eq!(storage.available, expected_storage.available);
        assert_eq!(storage.used, expected_storage.used);
    }

    #[tokio::test]
    async fn readiness_check_returns_ok_when_ready() {
        let response = ServerServiceImpl::new(test_dependencies(true))
            .readiness_check(Request::new(ReadinessCheckRequest { timeout: None }))
            .await
            .expect("readiness_check should succeed")
            .into_inner();

        assert!(matches!(
            response.result,
            Some(readiness_check_response::Result::OkResponse(_))
        ));
    }

    #[tokio::test]
    async fn readiness_check_returns_failure_when_not_ready() {
        let response = ServerServiceImpl::new(test_dependencies(false))
            .readiness_check(Request::new(ReadinessCheckRequest { timeout: None }))
            .await
            .expect("readiness_check should succeed")
            .into_inner();

        assert!(matches!(
            response.result,
            Some(readiness_check_response::Result::FailureResponse(_))
        ));
    }

    #[tokio::test]
    async fn server_signature_returns_signature_from_dependencies() {
        let dependencies = test_dependencies(true);
        let response = ServerServiceImpl::new(Arc::clone(&dependencies))
            .server_signature(Request::new(ServerSignatureRequest {}))
            .await
            .expect("server_signature should succeed")
            .into_inner();

        assert_eq!(
            response.public_key,
            dependencies.server_signature_public_key.as_slice()
        );
    }

    #[tokio::test]
    async fn default_uses_package_version() {
        let response = ServerServiceImpl::default()
            .server_info(Request::new(ServerInfoRequest {}))
            .await
            .expect("server_info should succeed")
            .into_inner();

        assert_eq!(response.server_version, env!("CARGO_PKG_VERSION"));
    }
}
