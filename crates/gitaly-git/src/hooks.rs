//! hook payload helpers shared by Git hook execution paths.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const ENV_HOOKS_PAYLOAD: &str = "GITALY_HOOKS_PAYLOAD";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HooksPayload {
    pub requested_hooks: u32,
    pub object_format: String,
    pub runtime_dir: String,
    pub internal_socket: String,
    pub internal_socket_token: String,
    #[serde(default)]
    pub feature_flags: BTreeMap<String, bool>,
    #[serde(default)]
    pub user_details: Option<UserDetails>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDetails {
    pub username: String,
    pub user_id: String,
    pub protocol: String,
    pub remote_ip: String,
}

impl HooksPayload {
    pub fn to_env_value(&self) -> Result<String, HooksPayloadError> {
        let json = serde_json::to_vec(self).map_err(HooksPayloadError::Serialize)?;
        Ok(BASE64_STANDARD.encode(json))
    }

    pub fn to_env_assignment(&self) -> Result<String, HooksPayloadError> {
        Ok(format!("{}={}", ENV_HOOKS_PAYLOAD, self.to_env_value()?))
    }
}

pub fn hooks_payload_from_env(envs: &[String]) -> Result<HooksPayload, HooksPayloadFromEnvError> {
    let encoded = lookup_env(envs, ENV_HOOKS_PAYLOAD).ok_or(HooksPayloadFromEnvError::NotFound)?;
    let decoded = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(HooksPayloadFromEnvError::Decode)?;

    serde_json::from_slice::<HooksPayload>(&decoded).map_err(HooksPayloadFromEnvError::Deserialize)
}

fn lookup_env<'a>(envs: &'a [String], key: &str) -> Option<&'a str> {
    envs.iter()
        .filter_map(|entry| entry.split_once('='))
        .find_map(|(k, v)| if k == key { Some(v) } else { None })
}

#[derive(Debug, Error)]
pub enum HooksPayloadError {
    #[error("failed to serialize hooks payload to JSON: {0}")]
    Serialize(serde_json::Error),
}

#[derive(Debug, Error)]
pub enum HooksPayloadFromEnvError {
    #[error("no hooks payload found in environment")]
    NotFound,
    #[error("failed to base64 decode hooks payload: {0}")]
    Decode(base64::DecodeError),
    #[error("failed to deserialize hooks payload JSON: {0}")]
    Deserialize(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use base64::Engine;

    use super::{
        hooks_payload_from_env, HooksPayload, HooksPayloadFromEnvError, UserDetails,
        ENV_HOOKS_PAYLOAD,
    };

    #[test]
    fn payload_env_roundtrip() {
        let payload = HooksPayload {
            requested_hooks: 0b1011,
            object_format: "sha1".to_string(),
            runtime_dir: "/var/opt/gitlab/gitaly".to_string(),
            internal_socket: "/var/opt/gitlab/gitaly/gitaly.socket".to_string(),
            internal_socket_token: "secret-token".to_string(),
            feature_flags: BTreeMap::from([
                ("new_reference_backend".to_string(), true),
                ("optimized_pack_objects".to_string(), false),
            ]),
            user_details: Some(UserDetails {
                username: "alice".to_string(),
                user_id: "42".to_string(),
                protocol: "ssh".to_string(),
                remote_ip: "127.0.0.1".to_string(),
            }),
        };

        let env_assignment = payload
            .to_env_assignment()
            .expect("payload should encode to env");
        let envs = vec![env_assignment];

        let decoded = hooks_payload_from_env(&envs).expect("payload should decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn payload_from_env_reports_missing_env() {
        let err = hooks_payload_from_env(&[]).expect_err("missing env should fail");
        assert!(matches!(err, HooksPayloadFromEnvError::NotFound));
    }

    #[test]
    fn payload_from_env_reports_decode_error() {
        let envs = vec![format!("{ENV_HOOKS_PAYLOAD}=not-base64%%%")];
        let err = hooks_payload_from_env(&envs).expect_err("invalid base64 should fail");
        assert!(matches!(err, HooksPayloadFromEnvError::Decode(_)));
    }

    #[test]
    fn payload_from_env_reports_json_error() {
        let invalid_json = base64::engine::general_purpose::STANDARD.encode(b"{invalid-json");
        let envs = vec![format!("{ENV_HOOKS_PAYLOAD}={invalid_json}")];

        let err = hooks_payload_from_env(&envs).expect_err("invalid json should fail");
        assert!(matches!(err, HooksPayloadFromEnvError::Deserialize(_)));
    }
}
