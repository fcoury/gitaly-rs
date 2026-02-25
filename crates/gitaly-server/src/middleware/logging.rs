use tonic::{Request, Status};
use tracing::info;

use super::metrics::RequestMetricSnapshot;

pub(crate) fn apply(request: Request<()>) -> Result<Request<()>, Status> {
    let fields = super::observability_fields(&request);
    if let Some(snapshot) = request.extensions().get::<RequestMetricSnapshot>() {
        info!(
            correlation_id = %fields.correlation_id,
            grpc.service = %fields.service,
            grpc.method = %fields.method,
            grpc.full_method = %fields.full_method,
            grpc.request_count = snapshot.total_seen,
            "incoming gRPC request"
        );
    } else {
        info!(
            correlation_id = %fields.correlation_id,
            grpc.service = %fields.service,
            grpc.method = %fields.method,
            grpc.full_method = %fields.full_method,
            "incoming gRPC request"
        );
    }

    super::mark_step(request, "logging")
}
