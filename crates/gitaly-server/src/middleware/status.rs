use tonic::{Request, Status};

pub(crate) fn apply(request: Request<()>) -> Result<Request<()>, Status> {
    super::mark_step(request, "status")
}
