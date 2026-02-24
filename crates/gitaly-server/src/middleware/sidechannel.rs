use tonic::{Request, Status};

const SIDECHANNEL_ID_METADATA_KEY: &str = "x-gitaly-sidechannel-id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SidechannelInfo {
    pub(crate) channel_id: Option<String>,
}

pub(crate) fn apply(mut request: Request<()>) -> Result<Request<()>, Status> {
    let channel_id = request
        .metadata()
        .get(SIDECHANNEL_ID_METADATA_KEY)
        .map(|value| {
            value
                .to_str()
                .map(ToString::to_string)
                .map_err(|_| Status::invalid_argument("invalid `x-gitaly-sidechannel-id` metadata"))
        })
        .transpose()?;

    request
        .extensions_mut()
        .insert(SidechannelInfo { channel_id });
    super::mark_step(request, "sidechannel")
}
