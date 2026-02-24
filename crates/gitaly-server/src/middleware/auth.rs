use tonic::{Request, Status};

use super::MiddlewareContext;

const AUTHORIZATION_METADATA_KEY: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthDecision {
    Disabled,
    Authenticated,
    TransitionAllowed,
}

pub(crate) fn apply(
    mut request: Request<()>,
    context: &MiddlewareContext,
) -> Result<Request<()>, Status> {
    let Some(expected_token) = context.auth_token() else {
        request.extensions_mut().insert(AuthDecision::Disabled);
        return super::mark_step(request, "auth");
    };

    let presented_token = request
        .metadata()
        .get(AUTHORIZATION_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix(BEARER_PREFIX));

    let decision = if presented_token.is_some_and(|token| token == expected_token) {
        AuthDecision::Authenticated
    } else if context.allow_unauthenticated() {
        AuthDecision::TransitionAllowed
    } else {
        return Err(Status::unauthenticated(
            "missing or invalid `authorization` token",
        ));
    };

    request.extensions_mut().insert(decision);
    super::mark_step(request, "auth")
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Request};

    use gitaly_limiter::concurrency::ConcurrencyLimiter;

    use super::{apply, AuthDecision};
    use crate::middleware::MiddlewareContext;

    #[test]
    fn apply_authenticates_valid_bearer_token() {
        let context = MiddlewareContext::new(ConcurrencyLimiter::new(1, 0))
            .with_auth_token("secret-token", false);
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            "Bearer secret-token"
                .parse()
                .expect("metadata should parse"),
        );

        let request = apply(request, &context).expect("auth should succeed");
        assert_eq!(
            request.extensions().get::<AuthDecision>(),
            Some(&AuthDecision::Authenticated)
        );
    }

    #[test]
    fn apply_rejects_missing_token_when_transition_is_disabled() {
        let context = MiddlewareContext::new(ConcurrencyLimiter::new(1, 0))
            .with_auth_token("secret-token", false);
        let request = Request::new(());

        let error = apply(request, &context).expect_err("auth should fail");
        assert_eq!(error.code(), Code::Unauthenticated);
    }

    #[test]
    fn apply_allows_missing_token_when_transition_is_enabled() {
        let context = MiddlewareContext::new(ConcurrencyLimiter::new(1, 0))
            .with_auth_token("secret-token", true);
        let request = Request::new(());

        let request = apply(request, &context).expect("transition mode should allow request");
        assert_eq!(
            request.extensions().get::<AuthDecision>(),
            Some(&AuthDecision::TransitionAllowed)
        );
    }
}
