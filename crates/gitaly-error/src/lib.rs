use std::collections::HashMap;

use thiserror::Error;
use tonic::{Code, Status};

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{code}: {message}")]
pub struct GitalyError {
    code: Code,
    message: String,
    metadata: HashMap<String, String>,
}

impl GitalyError {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn code(&self) -> Code {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl From<GitalyError> for Status {
    fn from(error: GitalyError) -> Self {
        let details = serde_json::to_vec(&error.metadata).unwrap_or_default();
        Status::with_details(error.code, error.message, details.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tonic::Code;

    use super::GitalyError;

    #[test]
    fn converts_to_status_with_code_and_message() {
        let err = GitalyError::new(Code::PermissionDenied, "access denied");
        let status: tonic::Status = err.into();

        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), "access denied");
    }

    #[test]
    fn metadata_roundtrips_via_status_details_json() {
        let err = GitalyError::new(Code::InvalidArgument, "invalid")
            .with_metadata("repo", "my/repo")
            .with_metadata("reason", "missing branch");
        let status: tonic::Status = err.into();

        let metadata: HashMap<String, String> = serde_json::from_slice(status.details())
            .expect("status details should contain valid metadata JSON");

        assert_eq!(metadata.get("repo"), Some(&"my/repo".to_string()));
        assert_eq!(metadata.get("reason"), Some(&"missing branch".to_string()));
    }
}
