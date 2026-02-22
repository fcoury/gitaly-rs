use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use time::Duration;
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenVersion {
    V1,
}

impl TokenVersion {
    fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
        }
    }

    fn parse(input: &str) -> Result<Self, Error> {
        match input {
            "v1" => Ok(Self::V1),
            _ => Err(Error::UnsupportedVersion),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenClaims {
    pub issued_at: OffsetDateTime,
    pub path: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("malformed token")]
    MalformedToken,
    #[error("unsupported token version")]
    UnsupportedVersion,
    #[error("invalid token signature")]
    InvalidSignature,
    #[error("token expired")]
    Expired,
    #[error("token issued too far in the future")]
    IssuedInFuture,
    #[error("token encoding error")]
    Encoding,
    #[error("internal error")]
    Internal,
}

pub type Error = AuthError;

#[derive(Debug, Clone)]
pub struct TokenSigner {
    secret: Vec<u8>,
    version: TokenVersion,
}

impl TokenSigner {
    pub fn new(secret: impl AsRef<[u8]>, version: TokenVersion) -> Self {
        Self {
            secret: secret.as_ref().to_vec(),
            version,
        }
    }

    pub fn sign(&self, claims: &TokenClaims) -> Result<String, Error> {
        let payload = payload(self.version, claims);
        let signature = sign_bytes(&self.secret, payload.as_bytes())?;
        let signature = URL_SAFE_NO_PAD.encode(signature);
        Ok(format!("{payload}.{signature}"))
    }
}

#[derive(Debug, Clone)]
pub struct TokenVerifier {
    secret: Vec<u8>,
}

impl TokenVerifier {
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            secret: secret.as_ref().to_vec(),
        }
    }

    pub fn verify(
        &self,
        token: &str,
        now: OffsetDateTime,
        max_skew_secs: i64,
    ) -> Result<TokenClaims, Error> {
        if max_skew_secs < 0 {
            return Err(Error::MalformedToken);
        }

        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 4 {
            return Err(Error::MalformedToken);
        }

        let version = TokenVersion::parse(parts[0])?;
        let issued_at_unix_nanos: i128 = parts[1].parse().map_err(|_| Error::MalformedToken)?;
        let issued_at = OffsetDateTime::from_unix_timestamp_nanos(issued_at_unix_nanos)
            .map_err(|_| Error::MalformedToken)?;
        let path_bytes = URL_SAFE_NO_PAD
            .decode(parts[2])
            .map_err(|_| Error::MalformedToken)?;
        let path = String::from_utf8(path_bytes).map_err(|_| Error::MalformedToken)?;

        let payload = format!("{}.{}.{}", version.as_str(), issued_at_unix_nanos, parts[2]);
        let signature = URL_SAFE_NO_PAD
            .decode(parts[3])
            .map_err(|_| Error::MalformedToken)?;
        verify_signature(&self.secret, payload.as_bytes(), &signature)?;

        let max_skew = Duration::seconds(max_skew_secs);
        if now - issued_at > max_skew {
            return Err(Error::Expired);
        }
        if issued_at - now > max_skew {
            return Err(Error::IssuedInFuture);
        }

        Ok(TokenClaims { issued_at, path })
    }
}

fn payload(version: TokenVersion, claims: &TokenClaims) -> String {
    let path = URL_SAFE_NO_PAD.encode(claims.path.as_bytes());
    let issued_at = claims.issued_at.unix_timestamp_nanos();
    format!("{}.{}.{}", version.as_str(), issued_at, path)
}

fn sign_bytes(secret: &[u8], payload: &[u8]) -> Result<Vec<u8>, Error> {
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| Error::Internal)?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_signature(secret: &[u8], payload: &[u8], signature: &[u8]) -> Result<(), Error> {
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| Error::Internal)?;
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| Error::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    fn signer() -> TokenSigner {
        TokenSigner::new("super-secret-key", TokenVersion::V1)
    }

    fn verifier() -> TokenVerifier {
        TokenVerifier::new("super-secret-key")
    }

    #[test]
    fn roundtrip_sign_and_verify() {
        let now = OffsetDateTime::now_utc();
        let claims = TokenClaims {
            issued_at: now,
            path: "/gitaly.SmartHTTPService/PostUploadPack".to_string(),
        };

        let token = signer().sign(&claims).expect("token signs");
        let verified = verifier()
            .verify(&token, now, 30)
            .expect("token verifies successfully");

        assert_eq!(verified, claims);
    }

    #[test]
    fn bad_signature_is_rejected() {
        let now = OffsetDateTime::now_utc();
        let claims = TokenClaims {
            issued_at: now,
            path: "/rpc".to_string(),
        };

        let token = signer().sign(&claims).expect("token signs");
        let mut parts = token
            .split('.')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 4);

        let mut sig_bytes = URL_SAFE_NO_PAD
            .decode(parts[3].as_bytes())
            .expect("signature should decode");
        sig_bytes[0] ^= 0x01;
        parts[3] = URL_SAFE_NO_PAD.encode(sig_bytes);
        let tampered = parts.join(".");

        let err = verifier()
            .verify(&tampered, now, 30)
            .expect_err("tampered token must be rejected");
        assert_eq!(err, Error::InvalidSignature);
    }

    #[test]
    fn expired_token_is_rejected() {
        let now = OffsetDateTime::now_utc();
        let claims = TokenClaims {
            issued_at: now - Duration::seconds(120),
            path: "/rpc".to_string(),
        };

        let token = signer().sign(&claims).expect("token signs");
        let err = verifier()
            .verify(&token, now, 60)
            .expect_err("expired token must be rejected");
        assert_eq!(err, Error::Expired);
    }

    #[test]
    fn malformed_token_is_rejected() {
        let err = verifier()
            .verify("not-a-token", OffsetDateTime::now_utc(), 60)
            .expect_err("malformed token must be rejected");
        assert_eq!(err, Error::MalformedToken);
    }
}
