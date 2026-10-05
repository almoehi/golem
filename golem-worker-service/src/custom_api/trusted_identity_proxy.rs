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
use chrono::{DateTime, TimeDelta, Utc};
use http::{HeaderMap, HeaderName, HeaderValue};
use openidconnect::{IssuerUrl, Scope};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use subtle::{Choice, ConstantTimeEq};
use tracing::debug;
use url::Url;

/// An asserted identity may not claim to stay valid for longer than this.
pub const MAX_ASSERTION_LIFETIME: TimeDelta = TimeDelta::hours(24);

/// How far in the future the `issued_at` of an asserted identity may lie.
pub const MAX_ISSUED_AT_SKEW: TimeDelta = TimeDelta::minutes(5);

/// Headers with a meaning of their own to the gateway, the agents or the tracing. They cannot
/// be used as the secret or the identity header: both are removed from every request.
pub const RESERVED_HEADERS: [&str; 12] = [
    "authorization",
    "cookie",
    "host",
    "origin",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-for",
    "traceparent",
    "tracestate",
    "baggage",
    "content-length",
    "content-type",
];

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrustedIdentityProxyConfigError {
    #[error(
        "trusted_identity_proxy.{field} is set but trusted_identity_proxy.enabled is not true; enable the trusted identity proxy or remove the setting"
    )]
    SettingWithoutEnabled { field: &'static str },
    #[error("trusted_identity_proxy is enabled but trusted_identity_proxy.secret is not set")]
    MissingSecret,
    #[error("trusted_identity_proxy.{field} must be at least {min_length} bytes long")]
    SecretTooShort {
        field: &'static str,
        min_length: usize,
    },
    #[error("trusted_identity_proxy.{field} is not a valid HTTP header name")]
    InvalidHeaderName { field: &'static str },
    #[error("trusted_identity_proxy.{field} must not be a header with a meaning of its own")]
    ReservedHeaderName { field: &'static str },
    #[error(
        "trusted_identity_proxy.secret_header and trusted_identity_proxy.identity_header must be different headers"
    )]
    HeadersNotDistinct,
    #[error(
        "trusted_identity_proxy is enabled but trusted_identity_proxy.allowed_issuers is empty"
    )]
    NoAllowedIssuers,
    #[error("trusted_identity_proxy.allowed_issuers entry {position} is not usable: {reason}")]
    InvalidAllowedIssuer {
        /// 1-based
        position: usize,
        reason: &'static str,
    },
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
    allowed_issuers: HashSet<String>,
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

    /// The one rule for every way of configuring the service: the feature is on exactly when
    /// `enabled` is true, and any other setting of the section without it is refused, so a
    /// configured secret can never sit in a service that neither checks nor removes it.
    pub fn from_config(
        config: &TrustedIdentityProxyConfig,
    ) -> Result<Self, TrustedIdentityProxyConfigError> {
        if !config.enabled {
            return match setting_requiring_enabled(config) {
                Some(field) => {
                    Err(TrustedIdentityProxyConfigError::SettingWithoutEnabled { field })
                }
                None => Ok(Self::disabled()),
            };
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

        if config.allowed_issuers.is_empty() {
            return Err(TrustedIdentityProxyConfigError::NoAllowedIssuers);
        }
        for (index, issuer) in config.allowed_issuers.iter().enumerate() {
            if let Some(reason) = issuer_defect(issuer) {
                return Err(TrustedIdentityProxyConfigError::InvalidAllowedIssuer {
                    position: index + 1,
                    reason,
                });
            }
        }

        Ok(Self {
            settings: Some(Settings {
                accepted_secrets,
                secret_header,
                identity_header,
                allowed_issuers: config.allowed_issuers.iter().cloned().collect(),
            }),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.settings.is_some()
    }

    /// First step of handling a request: removes the secret and identity headers and reports
    /// what they carried. Nothing may log, route, trace or bind the request before this, so
    /// neither header is ever visible to anything behind the gateway.
    pub fn strip_and_wrap(
        &self,
        mut request: poem::Request,
    ) -> (RichRequest, TrustedProxyCredentials) {
        let credentials = self.take_credentials(request.headers_mut());
        (RichRequest::new(request), credentials)
    }

    fn take_credentials(&self, headers: &mut HeaderMap) -> TrustedProxyCredentials {
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

    /// Applies the trusted identity proxy rules for the security kind of the route:
    /// - security scheme (OIDC) routes: a presented secret must be valid and come with a valid
    ///   identity, which then replaces cookie based authentication for this request
    /// - session-from-header routes: the identity is only taken from the identity header of a
    ///   request with a valid secret; the route's own session header is removed and ignored
    /// - routes without security: no effect
    ///
    /// Every rejection is a 401; nothing is stored and no cookie is set.
    pub fn apply_incoming_middleware(
        &self,
        request: &mut RichRequest,
        resolved_route: &ResolvedRouteEntry,
        credentials: TrustedProxyCredentials,
    ) -> Result<TrustedProxyAuthentication, RequestHandlerError> {
        use TrustedProxyCredentials::{Disabled, InvalidSecret, NotPresented, ValidSecret};

        let Some(settings) = &self.settings else {
            return Ok(TrustedProxyAuthentication::NotApplied);
        };

        let secret_is_optional = match &resolved_route.route.security {
            RichRouteSecurity::None => return Ok(TrustedProxyAuthentication::NotApplied),
            RichRouteSecurity::SecurityScheme(_) => true,
            RichRouteSecurity::SessionFromHeader(route_security) => {
                // The route's own header is a verbatim trusted identity: it must neither
                // authenticate the request nor reach the agent.
                request
                    .underlying
                    .headers_mut()
                    .remove(route_security.header_name.as_str());
                false
            }
        };

        match credentials {
            Disabled => Ok(TrustedProxyAuthentication::NotApplied),
            NotPresented if secret_is_optional => Ok(TrustedProxyAuthentication::NotApplied),
            NotPresented => Err(rejected("the trusted proxy secret is required")),
            InvalidSecret => Err(rejected("the trusted proxy secret is not valid")),
            ValidSecret { identity } => {
                let session = asserted_session(&identity, &settings.allowed_issuers, Utc::now())
                    .map_err(rejected)?;
                debug!("Using the identity asserted by the trusted identity proxy");
                request.set_authenticated_session(session);
                Ok(TrustedProxyAuthentication::Authenticated)
            }
        }
    }
}

/// The first setting that is only meaningful with the feature enabled, if any is set
fn setting_requiring_enabled(config: &TrustedIdentityProxyConfig) -> Option<&'static str> {
    if config.secret.is_some() {
        Some("secret")
    } else if config.previous_secret.is_some() {
        Some("previous_secret")
    } else if config.secret_header != TrustedIdentityProxyConfig::DEFAULT_SECRET_HEADER {
        Some("secret_header")
    } else if config.identity_header != TrustedIdentityProxyConfig::DEFAULT_IDENTITY_HEADER {
        Some("identity_header")
    } else if !config.allowed_issuers.is_empty() {
        Some("allowed_issuers")
    } else {
        None
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
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| TrustedIdentityProxyConfigError::InvalidHeaderName { field })?;
    if RESERVED_HEADERS.contains(&name.as_str()) {
        return Err(TrustedIdentityProxyConfigError::ReservedHeaderName { field });
    }
    Ok(name)
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

fn rejected(reason: &'static str) -> RequestHandlerError {
    debug!("Rejecting trusted identity proxy request: {reason}");
    RequestHandlerError::TrustedIdentityRejected { reason }
}

/// The identity JSON of the identity header. Unlike the test session header nothing is
/// defaulted or ignored: an unknown field (a misspelt `expires_at`, say) is an error.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertedIdentity {
    subject: String,
    issuer: String,

    email: Option<String>,
    name: Option<String>,
    email_verified: Option<bool>,
    given_name: Option<String>,
    family_name: Option<String>,
    picture: Option<String>,
    preferred_username: Option<String>,

    scopes: Option<HashSet<Scope>>,
    issued_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
}

impl AssertedIdentity {
    fn into_session(
        self,
        allowed_issuers: &HashSet<String>,
        now: DateTime<Utc>,
    ) -> Result<OidcSession, &'static str> {
        if identifier_defect(&self.subject).is_some() {
            return Err("the subject of the identity is not usable");
        }
        if issuer_defect(&self.issuer).is_some() {
            return Err("the issuer of the identity is not usable");
        }
        if !allowed_issuers.contains(&self.issuer) {
            return Err("the issuer of the identity is not an allowed issuer");
        }

        if let Some(issued_at) = self.issued_at
            && issued_at > now + MAX_ISSUED_AT_SKEW
        {
            return Err("the identity is issued in the future");
        }
        if let Some(expires_at) = self.expires_at {
            if expires_at <= now {
                return Err("the identity is expired");
            }
            if expires_at > now + MAX_ASSERTION_LIFETIME {
                return Err("the identity expires too far in the future");
            }
        }

        let issuer =
            IssuerUrl::new(self.issuer).map_err(|_| "the issuer of the identity is not usable")?;

        // The session, and with it the principal and its claims, is built exactly as for the
        // test session header.
        Ok(EasyOidcSession {
            subject: self.subject,
            issuer,
            email: self.email,
            name: self.name,
            email_verified: self.email_verified,
            given_name: self.given_name,
            family_name: self.family_name,
            picture: self.picture,
            preferred_username: self.preferred_username,
            scopes: self.scopes.unwrap_or_else(EasyOidcSession::default_scopes),
            issued_at: self
                .issued_at
                .unwrap_or_else(EasyOidcSession::default_issued_at),
            expires_at: self
                .expires_at
                .unwrap_or_else(EasyOidcSession::default_expires_at),
        }
        .into())
    }
}

fn asserted_session(
    identity: &[HeaderValue],
    allowed_issuers: &HashSet<String>,
    now: DateTime<Utc>,
) -> Result<OidcSession, &'static str> {
    let [identity] = identity else {
        return Err("exactly one identity header is required");
    };

    let identity: AssertedIdentity = serde_json::from_slice(identity.as_bytes())
        .map_err(|_| "the identity header is not a valid identity")?;

    identity.into_session(allowed_issuers, now)
}

/// Why a subject or issuer cannot be used. Applications key users by issuer and subject
/// joined with a separator, so neither may be empty, padded or contain control characters.
fn identifier_defect(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        Some("it is empty")
    } else if value.trim() != value {
        Some("it has leading or trailing whitespace")
    } else if value.chars().any(char::is_control) {
        Some("it contains control characters")
    } else {
        None
    }
}

/// Why an issuer cannot be used: besides the identifier rules it must be an http(s) URL in
/// the exact form it parses to.
fn issuer_defect(issuer: &str) -> Option<&'static str> {
    if let Some(defect) = identifier_defect(issuer) {
        Some(defect)
    } else if !issuer_url_round_trips(issuer) {
        Some("it is not an http(s) URL in normalized form")
    } else {
        None
    }
}

/// URL parsing drops tabs and line breaks and normalizes case, escapes and dots. An issuer is
/// only accepted if none of that happened, so two different strings can never mean one issuer.
/// The one tolerated difference is the `/` that parsing adds to a URL without a path.
fn issuer_url_round_trips(issuer: &str) -> bool {
    let Ok(url) = Url::parse(issuer) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }

    let without_path = url.path() == "/" && url.query().is_none() && url.fragment().is_none();
    url.as_str() == issuer || (without_path && url.as_str().strip_suffix('/') == Some(issuer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_r::test;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";
    const OTHER_SECRET: &str = "fedcba9876543210fedcba9876543210";
    const ISSUER: &str = "https://keys.example.com";

    fn config(secret: Option<&str>) -> TrustedIdentityProxyConfig {
        TrustedIdentityProxyConfig {
            enabled: true,
            secret: secret.map(TrustedProxySecret::new),
            allowed_issuers: vec![ISSUER.to_string()],
            ..Default::default()
        }
    }

    fn config_error(config: &TrustedIdentityProxyConfig) -> TrustedIdentityProxyConfigError {
        match TrustedIdentityProxy::from_config(config) {
            Ok(_) => panic!("configuration was accepted"),
            Err(error) => error,
        }
    }

    fn allowed() -> HashSet<String> {
        HashSet::from([ISSUER.to_string()])
    }

    fn session(identity: &str) -> Result<OidcSession, &'static str> {
        asserted_session(
            &[HeaderValue::from_bytes(identity.as_bytes()).unwrap()],
            &allowed(),
            Utc::now(),
        )
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
    fn any_setting_without_enabled_is_refused() {
        let defaults = TrustedIdentityProxyConfig::default;
        let cases = [
            (
                "secret",
                TrustedIdentityProxyConfig {
                    secret: Some(TrustedProxySecret::new(SECRET)),
                    ..defaults()
                },
            ),
            (
                "secret",
                TrustedIdentityProxyConfig {
                    secret: Some(TrustedProxySecret::new("")),
                    ..defaults()
                },
            ),
            (
                "previous_secret",
                TrustedIdentityProxyConfig {
                    previous_secret: Some(TrustedProxySecret::new(SECRET)),
                    ..defaults()
                },
            ),
            (
                "secret_header",
                TrustedIdentityProxyConfig {
                    secret_header: "X-Secret".to_string(),
                    ..defaults()
                },
            ),
            (
                "identity_header",
                TrustedIdentityProxyConfig {
                    identity_header: "X-Identity".to_string(),
                    ..defaults()
                },
            ),
            (
                "allowed_issuers",
                TrustedIdentityProxyConfig {
                    allowed_issuers: vec![ISSUER.to_string()],
                    ..defaults()
                },
            ),
        ];

        for (field, config) in cases {
            assert_eq!(
                config_error(&config),
                TrustedIdentityProxyConfigError::SettingWithoutEnabled { field }
            );
        }
    }

    #[test]
    fn enabled_without_secret_is_refused() {
        assert_eq!(
            config_error(&config(None)),
            TrustedIdentityProxyConfigError::MissingSecret
        );

        // neither a previous secret nor header names stand in for the secret
        let config = TrustedIdentityProxyConfig {
            previous_secret: Some(TrustedProxySecret::new(SECRET)),
            secret_header: "X-Secret".to_string(),
            ..config(None)
        };
        assert_eq!(
            config_error(&config),
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
    fn reserved_header_names_are_refused() {
        for reserved in RESERVED_HEADERS {
            // header names are case-insensitive
            let reserved = reserved.to_uppercase();

            let as_secret_header = TrustedIdentityProxyConfig {
                secret_header: reserved.clone(),
                ..config(Some(SECRET))
            };
            assert_eq!(
                config_error(&as_secret_header),
                TrustedIdentityProxyConfigError::ReservedHeaderName {
                    field: "secret_header"
                }
            );

            let as_identity_header = TrustedIdentityProxyConfig {
                identity_header: reserved,
                ..config(Some(SECRET))
            };
            assert_eq!(
                config_error(&as_identity_header),
                TrustedIdentityProxyConfigError::ReservedHeaderName {
                    field: "identity_header"
                }
            );
        }
    }

    #[test]
    fn enabled_without_allowed_issuers_is_refused() {
        let config = TrustedIdentityProxyConfig {
            allowed_issuers: vec![],
            ..config(Some(SECRET))
        };
        assert_eq!(
            config_error(&config),
            TrustedIdentityProxyConfigError::NoAllowedIssuers
        );
    }

    #[test]
    fn unusable_allowed_issuer_is_refused() {
        for issuer in ["keys", "foo:bar", "https://keys.example.com\n", " ", ""] {
            let config = TrustedIdentityProxyConfig {
                allowed_issuers: vec![ISSUER.to_string(), issuer.to_string()],
                ..config(Some(SECRET))
            };
            assert!(
                matches!(
                    config_error(&config),
                    TrustedIdentityProxyConfigError::InvalidAllowedIssuer { position: 2, .. }
                ),
                "{issuer:?}"
            );
        }
    }

    #[test]
    fn config_errors_do_not_contain_the_secret() {
        let secret = &SECRET[..31];
        let error = config_error(&config(Some(secret)));
        assert!(!error.to_string().contains(secret));

        let disabled = TrustedIdentityProxyConfig {
            secret: Some(TrustedProxySecret::new(SECRET)),
            ..Default::default()
        };
        assert!(!config_error(&disabled).to_string().contains(SECRET));
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

    #[test]
    fn non_utf8_secret_is_not_valid() {
        let proxy = TrustedIdentityProxy::from_config(&config(Some(SECRET))).unwrap();

        let mut bytes = SECRET.as_bytes().to_vec();
        bytes[0] = 0xff;
        let mut headers = HeaderMap::new();
        headers.insert(
            TrustedIdentityProxyConfig::DEFAULT_SECRET_HEADER,
            HeaderValue::from_bytes(&bytes).unwrap(),
        );

        assert!(matches!(
            proxy.take_credentials(&mut headers),
            TrustedProxyCredentials::InvalidSecret
        ));
        assert!(headers.is_empty());
    }

    #[test]
    fn non_utf8_identity_is_rejected() {
        let mut bytes = format!(r#"{{"subject":"user","issuer":"{ISSUER}"}}"#).into_bytes();
        let position = bytes.iter().position(|b| *b == b'u').unwrap();
        bytes[position] = 0xff;

        let result = asserted_session(
            &[HeaderValue::from_bytes(&bytes).unwrap()],
            &allowed(),
            Utc::now(),
        );
        assert_eq!(
            result.err(),
            Some("the identity header is not a valid identity")
        );
    }

    #[test]
    fn url_parsing_drops_tabs_and_line_breaks_which_the_round_trip_catches() {
        for separator in ["\t", "\n", "\r"] {
            let issuer = format!("https://keys.exa{separator}mple.com");

            // the premise: the parsed URL silently equals the clean issuer
            let parsed = Url::parse(&issuer).expect("parsed despite the separator");
            assert_eq!(parsed.as_str(), "https://keys.example.com/");

            // the round trip rejects it by itself, before the control character rule
            assert!(!issuer_url_round_trips(&issuer));
            assert!(issuer_defect(&issuer).is_some());
        }
    }

    #[test]
    fn issuer_must_be_a_normalized_http_url() {
        for usable in [
            "https://keys.example.com",
            "https://keys.example.com/",
            "http://localhost:8089/default",
            "https://keys.example.com/a/b?tenant=1",
        ] {
            assert_eq!(issuer_defect(usable), None, "{usable}");
        }

        for unusable in [
            "",
            "keys",
            "foo:bar",
            "ftp://keys.example.com",
            "mailto:robot@example.com",
            "https://KEYS.example.com",
            "HTTPS://keys.example.com",
            "https://keys.example.com/a/../b",
            "https://keys.example.com:443",
            "https://keys.example.com?tenant=1",
            " https://keys.example.com",
            "https://keys.example.com ",
            "https://keys.example.com\n",
            "https://keys.example.com/\u{0}",
        ] {
            assert!(issuer_defect(unusable).is_some(), "{unusable:?}");
        }
    }

    #[test]
    fn subject_must_not_be_empty_padded_or_contain_control_characters() {
        for usable in ["user", "auth0|1234", "a b", "ünïcode"] {
            assert_eq!(identifier_defect(usable), None, "{usable}");
        }
        for unusable in [
            "", " ", " user", "user ", "us\ner", "us\rer", "us\ter", "user\n", "\u{7f}", "a\u{85}b",
        ] {
            assert!(identifier_defect(unusable).is_some(), "{unusable:?}");
        }
    }

    #[test]
    fn asserted_identity_is_strict() {
        let valid = format!(r#"{{"subject":"user","issuer":"{ISSUER}"}}"#);
        let accepted = session(&valid).expect("valid identity");
        assert_eq!(accepted.subject, "user");
        assert_eq!(accepted.issuer, ISSUER);

        let now = Utc::now();
        let at = |delta: TimeDelta| (now + delta).to_rfc3339();
        let with = |extra: String| format!(r#"{{"subject":"user","issuer":"{ISSUER}",{extra}}}"#);

        let rejected = [
            // subject and issuer are required and never defaulted
            "{}".to_string(),
            format!(r#"{{"issuer":"{ISSUER}"}}"#),
            r#"{"subject":"user"}"#.to_string(),
            format!(r#"{{"subject":null,"issuer":"{ISSUER}"}}"#),
            format!(r#"{{"subject":"","issuer":"{ISSUER}"}}"#),
            format!(r#"{{"subject":" user","issuer":"{ISSUER}"}}"#),
            // separators inside either field
            format!(r#"{{"subject":"us\ner","issuer":"{ISSUER}"}}"#),
            format!(r#"{{"subject":"us\ter","issuer":"{ISSUER}"}}"#),
            format!(r#"{{"subject":"user","issuer":"{ISSUER}\n"}}"#),
            r#"{"subject":"user","issuer":"https://keys.exa\nmple.com"}"#.to_string(),
            // issuers that are not allowed or not URLs
            r#"{"subject":"user","issuer":"https://other.example.com"}"#.to_string(),
            format!(r#"{{"subject":"user","issuer":"{ISSUER}/"}}"#),
            r#"{"subject":"user","issuer":"foo:bar"}"#.to_string(),
            r#"{"subject":"user","issuer":"keys"}"#.to_string(),
            // unknown fields, such as other spellings of the expiry
            with(r#""exp":1"#.to_string()),
            with(format!(r#""expiresAt":"{}""#, at(TimeDelta::minutes(1)))),
            with(r#""sub":"other""#.to_string()),
            // time stamps
            with(r#""expires_at":1893456000"#.to_string()),
            with(format!(r#""expires_at":"{}""#, at(TimeDelta::seconds(-1)))),
            with(format!(r#""expires_at":"{}""#, at(TimeDelta::hours(25)))),
            with(r#""expires_at":"9999-12-31T23:59:59Z""#.to_string()),
            with(format!(r#""issued_at":"{}""#, at(TimeDelta::minutes(6)))),
            with(r#""issued_at":"9999-12-31T23:59:59Z""#.to_string()),
            // not JSON objects
            "user".to_string(),
            "[]".to_string(),
            "".to_string(),
        ];
        for identity in rejected {
            assert!(session(&identity).is_err(), "{identity}");
        }

        let accepted = [
            with(format!(r#""expires_at":"{}""#, at(TimeDelta::hours(23)))),
            with(format!(r#""issued_at":"{}""#, at(TimeDelta::minutes(4)))),
            with(format!(r#""issued_at":"{}""#, at(TimeDelta::days(-30)))),
            with(r#""email":null,"scopes":["openid","email"]"#.to_string()),
        ];
        for identity in accepted {
            assert!(session(&identity).is_ok(), "{identity}");
        }
    }

    #[test]
    fn issuer_must_be_allowed_exactly() {
        let allowed = HashSet::from(["https://keys.example.com/tenant".to_string()]);
        let assert = |issuer: &str| {
            let identity = format!(r#"{{"subject":"user","issuer":"{issuer}"}}"#);
            asserted_session(
                &[HeaderValue::from_str(&identity).unwrap()],
                &allowed,
                Utc::now(),
            )
        };

        assert!(assert("https://keys.example.com/tenant").is_ok());
        for other in [
            "https://keys.example.com/tenant/",
            "https://keys.example.com/Tenant",
            "https://keys.example.com",
            "http://keys.example.com/tenant",
        ] {
            assert_eq!(
                assert(other).err(),
                Some("the issuer of the identity is not an allowed issuer"),
                "{other}"
            );
        }
    }
}
