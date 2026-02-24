use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

pub(crate) const CORRELATION_ID_METADATA_KEY: &str = "x-correlation-id";

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CorrelationId(pub(crate) String);

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    let correlation_id = match request.metadata().get(CORRELATION_ID_METADATA_KEY) {
        Some(value) => {
            let value = value
                .to_str()
                .map_err(|_| Status::invalid_argument("invalid `x-correlation-id` header value"))?;
            if value.trim().is_empty() {
                generate_correlation_id()
            } else {
                value.to_string()
            }
        }
        None => generate_correlation_id(),
    };

    let metadata_value: MetadataValue<_> = correlation_id
        .parse()
        .map_err(|_| Status::invalid_argument("invalid `x-correlation-id` header value"))?;
    request
        .metadata_mut()
        .insert(CORRELATION_ID_METADATA_KEY, metadata_value);
    request
        .extensions_mut()
        .insert(CorrelationId(correlation_id));

    super::mark_step(request, "correlation_id")
}

fn generate_correlation_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let seq = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
    format!("gitaly-{now:x}-{seq:x}")
}

#[cfg(test)]
mod tests {
    use tonic::Request;

    use super::{apply, CorrelationId, CORRELATION_ID_METADATA_KEY};

    #[test]
    fn apply_reuses_existing_correlation_id() {
        let mut request = Request::new(());
        request.metadata_mut().insert(
            CORRELATION_ID_METADATA_KEY,
            "existing-id".parse().expect("metadata should parse"),
        );

        let request = apply(request).expect("apply should succeed");
        let value = request
            .metadata()
            .get(CORRELATION_ID_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .expect("correlation id should be present");
        assert_eq!(value, "existing-id");
        assert_eq!(
            request.extensions().get::<CorrelationId>(),
            Some(&CorrelationId("existing-id".to_string()))
        );
    }

    #[test]
    fn apply_generates_correlation_id_when_missing() {
        let request = Request::new(());
        let request = apply(request).expect("apply should succeed");
        let value = request
            .metadata()
            .get(CORRELATION_ID_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .expect("correlation id should be present");

        assert!(value.starts_with("gitaly-"));
        assert_eq!(
            request.extensions().get::<CorrelationId>(),
            Some(&CorrelationId(value.to_string()))
        );
    }
}
