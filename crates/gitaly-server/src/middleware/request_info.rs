use tonic::{GrpcMethod, Request, Status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestInfo {
    pub(crate) service: String,
    pub(crate) method: String,
    pub(crate) full_method: String,
}

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    let info = request
        .extensions()
        .get::<GrpcMethod<'static>>()
        .map_or_else(
            || RequestInfo {
                service: "unknown".to_string(),
                method: "unknown".to_string(),
                full_method: "/unknown/unknown".to_string(),
            },
            |grpc_method| {
                let service = grpc_method.service().to_string();
                let method = grpc_method.method().to_string();
                RequestInfo {
                    full_method: format!("/{service}/{method}"),
                    service,
                    method,
                }
            },
        );

    request.extensions_mut().insert(info);
    super::mark_step(request, "request_info")
}

#[cfg(test)]
mod tests {
    use tonic::{GrpcMethod, Request};

    use super::{apply, RequestInfo};

    #[test]
    fn apply_populates_request_info_from_grpc_extension() {
        let mut request = Request::new(());
        request.extensions_mut().insert(GrpcMethod::new(
            "gitaly.RepositoryService",
            "RepositoryExists",
        ));

        let request = apply(request).expect("apply should succeed");
        assert_eq!(
            request.extensions().get::<RequestInfo>(),
            Some(&RequestInfo {
                service: "gitaly.RepositoryService".to_string(),
                method: "RepositoryExists".to_string(),
                full_method: "/gitaly.RepositoryService/RepositoryExists".to_string(),
            })
        );
    }
}
