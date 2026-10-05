// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source License v1.1 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::error::RequestHandlerError;
use super::route_resolver::ResolvedRouteEntry;
use super::session_from_header_security::EasyOidcSession;
use super::{OidcSession, RichRequest, RichRouteSecurity};
use crate::config::{TrustedIdentityProxyConfig, TrustedProxySecret};
use http::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use subtle::{Choice, ConstantTimeEq};
use tracing::debug;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrustedIdentityProxyConfigError {
    #[error("trusted_identity_proxy is enabled but trusted_identity_proxy.secret is not set")]
    MissingSecret,
    #[error("trusted_identity_proxy.{field} must be at least {min_length} bytes long")]
    SecretTooShort {
        field: &'static str,
        min_length: usize,
    },
    #[error("trusted_identity_proxy.{field} is not a valid HTTP header name")]
    InvalidHeaderName { field: &'static str },
    #[error(
        "trusted_identity_proxy.secret_header and trusted_identity_proxy.identity_header must be different headers"
    )]
    HeadersNotDistinct,
}

/// SHA-256 of a shared secret. Comparing digests instead of the secrets keeps the
/// comparison independent of the length of either side.
struct SecretDigest([u8; 32]);

impl SecretDigest {
    fn of(secret: &[u8]) -> Self {
        Self(Sha256::digest(secret).into())
    }

    /// Constant-time comparison with a presented secret
    fn matches(&self, presented: &[u8]) -> Choice {
        self.0.ct_eq(&Self::of(presented).0)
    }
}

struct Settings {
    accepted_secrets: Vec<SecretDigest>,
    secret_header: HeaderName,
    identity_header: HeaderName,
}

impl Settings {
    fn accepts(&self, presented_secret: &[u8]) -> bool {
        self.accepted_secrets
            .iter()
            .fold(Choice::from(0), |accepted, secret| {
                accepted | secret.matches(presented_secret)
            })
            .into()
    }
}

/// What a request presented in the trusted identity proxy headers. Never holds the secret.
pub enum TrustedProxyCredentials {
    /// The trusted identity proxy is not enabled
    Disabled,
    /// The request carried no secret header
    NotPresented,
    /// The request carried a secret header that is not an accepted secret
    InvalidSecret,
    /// The request proved knowledge of an accepted secret
    ValidSecret {
        /// Every value of the identity header
        identity: Vec<HeaderValue>,
    },
}

/// Accepts identities asserted by a reverse proxy that proves knowledge of a shared secret.
pub struct TrustedIdentityProxy {
    settings: Option<Settings>,
}

impl TrustedIdentityProxy {
    pub fn disabled() -> Self {
        Self { settings: None }
    }

    pub fn from_config(
        config: &TrustedIdentityProxyConfig,
    ) -> Result<Self, TrustedIdentityProxyConfigError> {
        if !config.enabled {
            return Ok(Self::disabled());
        }

        let secret = config
            .secret
            .as_ref()
            .ok_or(TrustedIdentityProxyConfigError::MissingSecret)?;

        let mut accepted_secrets = vec![accepted_secret("secret", secret)?];
        if let Some(previous_secret) = &config.previous_secret {
            accepted_secrets.push(accepted_secret("previous_secret", previous_secret)?);
        }

        let secret_header = header_name("secret_header", &config.secret_header)?;
        let identity_header = header_name("identity_header", &config.identity_header)?;
        if secret_header == identity_header {
            return Err(TrustedIdentityProxyConfigError::HeadersNotDistinct);
        }

        Ok(Self {
            settings: Some(Settings {
                accepted_secrets,
                secret_header,
                identity_header,
            }),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.settings.is_some()
    }

    /// Removes the secret and identity headers from the request headers and reports what they
    /// carried. Must run before the request can be logged, traced or bound to agent parameters,
    /// so neither header is ever visible to anything behind the gateway.
    pub fn take_credentials(&self, headers: &mut HeaderMap) -> TrustedProxyCredentials {
        let Some(settings) = &self.settings else {
            return TrustedProxyCredentials::Disabled;
        };

        let secrets = take_header_values(headers, &settings.secret_header);
        let identity = take_header_values(headers, &settings.identity_header);

        match secrets.as_slice() {
            [] => TrustedProxyCredentials::NotPresented,
            [secret] if settings.accepts(secret.as_bytes()) => {
                TrustedProxyCredentials::ValidSecret { identity }
            }
            _ => TrustedProxyCredentials::InvalidSecret,
        }
    }
}

fn accepted_secret(
    field: &'static str,
    secret: &TrustedProxySecret,
) -> Result<SecretDigest, TrustedIdentityProxyConfigError> {
    let secret = secret.expose();
    if secret.len() < TrustedProxySecret::MIN_LENGTH {
        return Err(TrustedIdentityProxyConfigError::SecretTooShort {
            field,
            min_length: TrustedProxySecret::MIN_LENGTH,
        });
    }
    Ok(SecretDigest::of(secret))
}

fn header_name(
    field: &'static str,
    name: &str,
) -> Result<HeaderName, TrustedIdentityProxyConfigError> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| TrustedIdentityProxyConfigError::InvalidHeaderName { field })
}

fn take_header_values(headers: &mut HeaderMap, name: &HeaderName) -> Vec<HeaderValue> {
    let values = headers.get_all(name).iter().cloned().collect();
    headers.remove(name);
    values
}

/// Whether the trusted identity proxy authenticated the request
#[derive(Debug, PartialEq, Eq)]
pub enum TrustedProxyAuthentication {
    /// The regular security middlewares of the route decide
    NotApplied,
    /// The request carries the identity asserted by the proxy, the regular security
    /// middlewares of the route must be skipped
    Authenticated,
}

/// Applies the trusted identity proxy rules for the security kind of the route:
/// - security scheme (OIDC) routes: a presented secret must be valid and come with a valid
///   identity, which then replaces cookie based authentication for this request
/// - session-from-header routes: the route's own session header is only honoured together with
///   a valid secret
/// - routes without security: no effect
///
/// Every rejection is a 401; nothing is stored and no cookie is set.
pub fn apply_trusted_identity_proxy_middleware(
    request: &mut RichRequest,
    resolved_route: &ResolvedRouteEntry,
    credentials: TrustedProxyCredentials,
) -> Result<TrustedProxyAuthentication, RequestHandlerError> {
    use TrustedProxyAuthentication::{Authenticated, NotApplied};
    use TrustedProxyCredentials::{Disabled, InvalidSecret, NotPresented, ValidSecret};

    match (&resolved_route.route.security, credentials) {
        (_, Disabled) | (RichRouteSecurity::None, _) => Ok(NotApplied),

        (RichRouteSecurity::SecurityScheme(_), NotPresented) => Ok(NotApplied),
        (RichRouteSecurity::SecurityScheme(_), ValidSecret { identity }) => {
            let session = asserted_session(&identity).map_err(rejected)?;
            debug!("Using the identity asserted by the trusted identity proxy");
            request.set_authenticated_session(session);
            Ok(Authenticated)
        }

        (RichRouteSecurity::SessionFromHeader(_), ValidSecret { .. }) => Ok(NotApplied),
        (RichRouteSecurity::SessionFromHeader(_), NotPresented) => {
            Err(rejected("the trusted proxy secret is required"))
        }

        (
            RichRouteSecurity::SecurityScheme(_) | RichRouteSecurity::SessionFromHeader(_),
            InvalidSecret,
        ) => Err(rejected("the trusted proxy secret is not valid")),
    }
}

fn rejected(reason: &'static str) -> RequestHandlerError {
    debug!("Rejecting trusted identity proxy request: {reason}");
    RequestHandlerError::TrustedIdentityRejected { reason }
}

fn asserted_session(identity: &[HeaderValue]) -> Result<OidcSession, &'static str> {
    let [identity] = identity else {
        return Err("exactly one identity header is required");
    };

    let identity: EasyOidcSession = serde_json::from_slice(identity.as_bytes())
        .map_err(|_| "the identity header is not a valid identity")?;

    let session = identity
        .into_explicit_session()
        .ok_or("the identity must state a subject and an issuer")?;

    if session.is_expired() {
        return Err("the identity is expired");
    }

    Ok(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_r::test;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";
    const OTHER_SECRET: &str = "fedcba9876543210fedcba9876543210";

    fn config(secret: Option<&str>) -> TrustedIdentityProxyConfig {
        TrustedIdentityProxyConfig {
            enabled: true,
            secret: secret.map(TrustedProxySecret::new),
            ..Default::default()
        }
    }

    fn config_error(config: &TrustedIdentityProxyConfig) -> TrustedIdentityProxyConfigError {
        match TrustedIdentityProxy::from_config(config) {
            Ok(_) => panic!("configuration was accepted"),
            Err(error) => error,
        }
    }

    #[test]
    fn secret_digest_matches_only_the_same_secret() {
        let digest = SecretDigest::of(SECRET.as_bytes());

        assert!(bool::from(digest.matches(SECRET.as_bytes())));
        assert!(!bool::from(digest.matches(OTHER_SECRET.as_bytes())));
        assert!(!bool::from(digest.matches(&SECRET.as_bytes()[..31])));
        assert!(!bool::from(digest.matches(format!("{SECRET}0").as_bytes())));
        assert!(!bool::from(digest.matches(b"")));
    }

    #[test]
    fn default_config_is_disabled() {
        let proxy = TrustedIdentityProxy::from_config(&TrustedIdentityProxyConfig::default())
            .expect("default config is valid");
        assert!(!proxy.is_enabled());
    }

    #[test]
    fn disabled_config_is_not_validated() {
        let config = TrustedIdentityProxyConfig {
            enabled: false,
            secret: Some(TrustedProxySecret::new("short")),
            ..Default::default()
        };
        assert!(
            !TrustedIdentityProxy::from_config(&config)
                .unwrap()
                .is_enabled()
        );
    }

    #[test]
    fn enabled_without_secret_is_refused() {
        assert_eq!(
            config_error(&config(None)),
            TrustedIdentityProxyConfigError::MissingSecret
        );
    }

    #[test]
    fn enabled_with_empty_or_short_secret_is_refused() {
        for secret in ["", &SECRET[..31]] {
            assert_eq!(
                config_error(&config(Some(secret))),
                TrustedIdentityProxyConfigError::SecretTooShort {
                    field: "secret",
                    min_length: 32
                }
            );
        }
    }

    #[test]
    fn short_previous_secret_is_refused() {
        let config = TrustedIdentityProxyConfig {
            previous_secret: Some(TrustedProxySecret::new("short")),
            ..config(Some(SECRET))
        };
        assert_eq!(
            config_error(&config),
            TrustedIdentityProxyConfigError::SecretTooShort {
                field: "previous_secret",
                min_length: 32
            }
        );
    }

    #[test]
    fn invalid_or_identical_header_names_are_refused() {
        let invalid = TrustedIdentityProxyConfig {
            secret_header: "not a header".to_string(),
            ..config(Some(SECRET))
        };
        assert_eq!(
            config_error(&invalid),
            TrustedIdentityProxyConfigError::InvalidHeaderName {
                field: "secret_header"
            }
        );

        let identical = TrustedIdentityProxyConfig {
            secret_header: "X-Same".to_string(),
            identity_header: "x-same".to_string(),
            ..config(Some(SECRET))
        };
        assert_eq!(
            config_error(&identical),
            TrustedIdentityProxyConfigError::HeadersNotDistinct
        );
    }

    #[test]
    fn config_errors_do_not_contain_the_secret() {
        let secret = &SECRET[..31];
        let error = config_error(&config(Some(secret)));
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn previous_secret_is_accepted_during_rotation() {
        let config = TrustedIdentityProxyConfig {
            previous_secret: Some(TrustedProxySecret::new(OTHER_SECRET)),
            ..config(Some(SECRET))
        };
        let proxy = TrustedIdentityProxy::from_config(&config).unwrap();

        for (secret, accepted) in [(SECRET, true), (OTHER_SECRET, true), ("neither", false)] {
            let mut headers = HeaderMap::new();
            headers.insert(
                TrustedIdentityProxyConfig::DEFAULT_SECRET_HEADER,
                HeaderValue::from_static(secret),
            );
            let credentials = proxy.take_credentials(&mut headers);
            assert_eq!(
                matches!(credentials, TrustedProxyCredentials::ValidSecret { .. }),
                accepted
            );
        }
    }
}
