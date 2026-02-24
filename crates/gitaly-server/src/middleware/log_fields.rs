use tonic::{Request, Status};

use super::correlation_id::CorrelationId;
use super::request_info::RequestInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogFields {
    pub(crate) correlation_id: String,
    pub(crate) service: String,
    pub(crate) method: String,
    pub(crate) full_method: String,
}

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    let correlation_id = request
        .extensions()
        .get::<CorrelationId>()
        .map(|correlation_id| correlation_id.0.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let request_info = request
        .extensions()
        .get::<RequestInfo>()
        .cloned()
        .unwrap_or(RequestInfo {
            service: "unknown".to_string(),
            method: "unknown".to_string(),
            full_method: "/unknown/unknown".to_string(),
        });

    request.extensions_mut().insert(LogFields {
        correlation_id,
        service: request_info.service,
        method: request_info.method,
        full_method: request_info.full_method,
    });

    super::mark_step(request, "log_fields")
}
