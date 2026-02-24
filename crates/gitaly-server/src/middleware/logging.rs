use tonic::{Request, Status};
use tracing::info;

use super::log_fields::LogFields;

pub(crate) fn apply(request: Request<()>) -> Result<Request<()>, Status> {
    if let Some(fields) = request.extensions().get::<LogFields>() {
        info!(
            correlation_id = %fields.correlation_id,
            grpc.service = %fields.service,
            grpc.method = %fields.method,
            grpc.full_method = %fields.full_method,
            "incoming gRPC request"
        );
    } else {
        info!("incoming gRPC request");
    }

    super::mark_step(request, "logging")
}
