use std::time::Instant;

use tonic::{Request, Status};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct RequestStart(pub(crate) Instant);

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    request
        .extensions_mut()
        .insert(RequestStart(Instant::now()));
    super::mark_step(request, "status")
}
