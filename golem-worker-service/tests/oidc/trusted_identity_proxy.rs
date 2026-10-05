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

use crate::handler::{FakeIdentityProvider, sample_security_scheme};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use figment::Figment;
use figment::providers::Serialized;
use golem_common::SafeDisplay;
use golem_common::model::agent::{
    AgentTypeName, DataSchema, NamedElementSchemas, Principal, UntypedElementValue,
};
use golem_common::model::component::{ComponentId, ComponentRevision};
use golem_common::model::domain_registration::Domain;
use golem_service_base::custom_api::{
    CallAgentBehaviour, CorsOptions, MethodParameter, PathSegment, PathSegmentType,
    QueryOrHeaderType, RequestBodySchema, SessionFromHeaderRouteSecurity, WebhookCallbackBehaviour,
};
use golem_worker_service::api::common::ApiEndpointError;
use golem_worker_service::config::{
    TrustedIdentityProxyConfig, TrustedProxySecret, WorkerServiceConfig,
};
use golem_worker_service::custom_api::call_agent::{CallAgentHandler, principal_from_request};
use golem_worker_service::custom_api::error::RequestHandlerError;
use golem_worker_service::custom_api::oidc::handler::OidcHandler;
use golem_worker_service::custom_api::oidc::model::{
    McpPendingAuth, McpProxyCodeEntry, PendingOidcLogin, SessionId,
};
use golem_worker_service::custom_api::oidc::session_store::{SessionStore, SessionStoreError};
use golem_worker_service::custom_api::request_handler::apply_incoming_security_middlewares;
use golem_worker_service::custom_api::route_resolver::ResolvedRouteEntry;
use golem_worker_service::custom_api::trusted_identity_proxy::TrustedIdentityProxy;
use golem_worker_service::custom_api::{
    OidcSession, ParsedRequestBody, RichCompiledRoute, RichRequest, RichRouteBehaviour,
    RichRouteSecurity, RichSecuritySchemeRouteSecurity, RouteExecutionResult,
};
use http::{Method, StatusCode};
use openidconnect::core::CoreIdTokenClaims;
use openidconnect::{EmptyAdditionalClaims, IssuerUrl, StandardClaims, SubjectIdentifier};
use poem::IntoResponse;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use test_r::test;
use uuid::Uuid;

pub const SECRET: &str = "trusted-proxy-secret-0123456789abcdef";
pub const WRONG_SECRET: &str = "trusted-proxy-secret-0123456789abcdeX";
pub const SECRET_HEADER: &str = TrustedIdentityProxyConfig::DEFAULT_SECRET_HEADER;
pub const IDENTITY_HEADER: &str = TrustedIdentityProxyConfig::DEFAULT_IDENTITY_HEADER;
const ROUTE_SESSION_HEADER: &str = "X-Test-Session";
pub const ISSUER: &str = "https://keys.example.com";
pub const IDENTITY: &str = r#"{"subject":"api-key-user","issuer":"https://keys.example.com","email":"robot@example.com","email_verified":true,"name":"Robot"}"#;

/// Session store that counts every access, so tests can prove the store was not touched.
#[derive(Default)]
pub struct RecordingSessionStore {
    pending_logins: Mutex<HashMap<String, PendingOidcLogin>>,
    sessions: Mutex<HashMap<Uuid, OidcSession>>,
    accesses: Mutex<usize>,
}

impl RecordingSessionStore {
    fn record_access(&self) {
        *self.accesses.lock().unwrap() += 1;
    }

    pub fn accesses(&self) -> usize {
        *self.accesses.lock().unwrap()
    }

    fn pending_login_count(&self) -> usize {
        self.pending_logins.lock().unwrap().len()
    }

    fn session_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

#[async_trait]
impl SessionStore for RecordingSessionStore {
    async fn store_pending_oidc_login(
        &self,
        state: &str,
        login: PendingOidcLogin,
    ) -> Result<(), SessionStoreError> {
        self.record_access();
        self.pending_logins
            .lock()
            .unwrap()
            .insert(state.to_string(), login);
        Ok(())
    }

    async fn take_pending_oidc_login(
        &self,
        state: &str,
    ) -> Result<Option<PendingOidcLogin>, SessionStoreError> {
        self.record_access();
        Ok(self.pending_logins.lock().unwrap().remove(state))
    }

    async fn store_authenticated_session(
        &self,
        session_id: &SessionId,
        session: OidcSession,
    ) -> Result<(), SessionStoreError> {
        self.record_access();
        self.sessions.lock().unwrap().insert(session_id.0, session);
        Ok(())
    }

    async fn get_authenticated_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<OidcSession>, SessionStoreError> {
        self.record_access();
        Ok(self.sessions.lock().unwrap().get(&session_id.0).cloned())
    }

    async fn store_mcp_pending_auth(
        &self,
        _state: &str,
        _pending: McpPendingAuth,
    ) -> Result<(), SessionStoreError> {
        unimplemented!("MCP is not part of the custom API security middlewares")
    }

    async fn take_mcp_pending_auth(
        &self,
        _state: &str,
    ) -> Result<Option<McpPendingAuth>, SessionStoreError> {
        unimplemented!("MCP is not part of the custom API security middlewares")
    }

    async fn store_mcp_proxy_code(
        &self,
        _code: &str,
        _entry: McpProxyCodeEntry,
    ) -> Result<(), SessionStoreError> {
        unimplemented!("MCP is not part of the custom API security middlewares")
    }

    async fn take_mcp_proxy_code(
        &self,
        _code: &str,
    ) -> Result<Option<McpProxyCodeEntry>, SessionStoreError> {
        unimplemented!("MCP is not part of the custom API security middlewares")
    }
}

/// The gateway as far as authentication is concerned: the trusted identity proxy, the OIDC
/// handler and the session store behind it.
pub struct Gateway {
    proxy: TrustedIdentityProxy,
    store: Arc<RecordingSessionStore>,
    oidc_handler: OidcHandler,
}

/// A request after it went through the gateway's authentication
struct Authenticated {
    request: RichRequest,
    result: Result<Option<RouteExecutionResult>, RequestHandlerError>,
}

impl Gateway {
    fn new(config: &TrustedIdentityProxyConfig) -> Self {
        let store = Arc::new(RecordingSessionStore::default());
        Self {
            proxy: TrustedIdentityProxy::from_config(config).expect("valid configuration"),
            oidc_handler: OidcHandler::new(store.clone(), Arc::new(FakeIdentityProvider)),
            store,
        }
    }

    fn disabled() -> Self {
        Self::new(&TrustedIdentityProxyConfig::default())
    }

    fn enabled() -> Self {
        Self::new(&enabled_config())
    }

    /// The two steps `RequestHandler::handle_request` takes before executing a route: its
    /// own first step, then its security middlewares.
    async fn authenticate(
        &self,
        route: &ResolvedRouteEntry,
        headers: &[(&str, &str)],
    ) -> Authenticated {
        let mut request = poem::Request::builder().uri_str("/protected");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }

        let (mut request, credentials) = self.proxy.strip_and_wrap(request.finish());

        let result = apply_incoming_security_middlewares(
            &self.proxy,
            &self.oidc_handler,
            &mut request,
            route,
            credentials,
        )
        .await;

        Authenticated { request, result }
    }

    /// Stores a browser session and returns its `Cookie` header value
    fn login(&self, subject: &str) -> String {
        let session_id = Uuid::now_v7();
        self.store
            .sessions
            .lock()
            .unwrap()
            .insert(session_id, cookie_session(subject));
        format!("golem_session_id={session_id}")
    }
}

impl Authenticated {
    fn principal(&self) -> Principal {
        principal_from_request(&self.request).expect("principal")
    }

    /// The route may be executed as the given subject
    fn assert_passed_as(&self, subject: &str) {
        assert!(matches!(self.result, Ok(None)), "{:?}", self.result);
        let Principal::Oidc(principal) = self.principal() else {
            panic!("expected an OIDC principal");
        };
        assert_eq!(principal.sub, subject);
    }

    fn assert_redirected_to_identity_provider(&self) {
        let Ok(Some(response)) = &self.result else {
            panic!("expected a redirect, got {:?}", self.result);
        };
        assert_eq!(response.status, StatusCode::FOUND);
        assert!(response.headers[&http::header::LOCATION].contains("https://fake-idp/auth"));
        assert!(self.request.authenticated_session().is_none());
    }

    /// The request was answered with a 401 JSON error that reveals none of the credentials
    async fn assert_unauthorized(self) {
        let Err(error) = self.result else {
            panic!("expected a rejection, got {:?}", self.result);
        };
        assert!(
            matches!(error, RequestHandlerError::TrustedIdentityRejected { .. }),
            "{error:?}"
        );
        assert!(self.request.authenticated_session().is_none());

        let response = ApiEndpointError::from(error).into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(!response.headers().contains_key(http::header::LOCATION));
        assert!(!response.headers().contains_key(http::header::SET_COOKIE));
        assert!(!response.headers().contains_key(SECRET_HEADER));
        assert!(!response.headers().contains_key(IDENTITY_HEADER));

        let body = response.into_body().into_string().await.unwrap();
        let json: serde_json::Value = serde_json::from_str(&body).expect("JSON error body");
        assert_eq!(json["code"], "AUTH_UNAUTHORIZED");
        for sensitive in [SECRET, WRONG_SECRET, "api-key-user", "robot@example.com"] {
            assert!(!body.contains(sensitive), "{body}");
        }
    }
}

pub fn enabled_config() -> TrustedIdentityProxyConfig {
    TrustedIdentityProxyConfig {
        enabled: true,
        secret: Some(TrustedProxySecret::new(SECRET)),
        allowed_issuers: vec![ISSUER.to_string()],
        ..Default::default()
    }
}

fn cookie_session(subject: &str) -> OidcSession {
    let issuer = IssuerUrl::new("https://issuer.example".to_string()).unwrap();
    OidcSession {
        subject: subject.to_string(),
        issuer: issuer.to_string(),
        email: None,
        name: None,
        email_verified: None,
        given_name: None,
        family_name: None,
        picture: None,
        preferred_username: None,
        claims: CoreIdTokenClaims::new(
            issuer,
            vec![],
            Utc::now() + TimeDelta::hours(8),
            Utc::now(),
            StandardClaims::new(SubjectIdentifier::new(subject.to_string())),
            EmptyAdditionalClaims {},
        ),
        scopes: HashSet::new(),
        expires_at: Utc::now() + TimeDelta::hours(8),
    }
}

fn route(security: RichRouteSecurity, behavior: RichRouteBehaviour) -> ResolvedRouteEntry {
    ResolvedRouteEntry {
        domain: Domain("example.com".to_string()),
        route: Arc::new(RichCompiledRoute {
            account_id: Default::default(),
            environment_id: Default::default(),
            route_id: 1,
            method: Method::GET,
            path: vec![PathSegment::Literal {
                value: "protected".to_string(),
            }],
            body: RequestBodySchema::Unused,
            behavior,
            security,
            cors: CorsOptions {
                allowed_patterns: vec![],
            },
        }),
        captured_path_parameters: vec![],
        openapi_spec: None,
    }
}

fn any_behaviour() -> RichRouteBehaviour {
    RichRouteBehaviour::WebhookCallback(WebhookCallbackBehaviour {
        component_id: ComponentId::new(),
    })
}

fn oidc_route() -> ResolvedRouteEntry {
    route(
        RichRouteSecurity::SecurityScheme(RichSecuritySchemeRouteSecurity {
            security_scheme: sample_security_scheme(),
        }),
        any_behaviour(),
    )
}

fn session_from_header_route() -> ResolvedRouteEntry {
    route(
        RichRouteSecurity::SessionFromHeader(SessionFromHeaderRouteSecurity {
            header_name: ROUTE_SESSION_HEADER.to_string(),
        }),
        any_behaviour(),
    )
}

fn open_route() -> ResolvedRouteEntry {
    route(RichRouteSecurity::None, any_behaviour())
}

fn call_agent_behaviour(
    bound_header: &str,
    parameter_type: QueryOrHeaderType,
) -> CallAgentBehaviour {
    CallAgentBehaviour {
        component_id: ComponentId::new(),
        component_revision: ComponentRevision::INITIAL,
        agent_type: AgentTypeName("test-agent".to_string()),
        constructor_parameters: vec![],
        phantom: false,
        method_name: "whoami".to_string(),
        method_parameters: vec![MethodParameter::Header {
            header_name: bound_header.to_string(),
            parameter_type,
        }],
        expected_agent_response: DataSchema::Tuple(NamedElementSchemas { elements: vec![] }),
        method_description: None,
    }
}

fn optional_string() -> QueryOrHeaderType {
    QueryOrHeaderType::Option {
        name: None,
        owner: None,
        inner: Box::new(PathSegmentType::Str),
    }
}

// --- disabled (default) ---

#[test]
async fn disabled_oidc_route_ignores_the_proxy_headers() {
    let gateway = Gateway::disabled();

    let authenticated = gateway
        .authenticate(
            &oidc_route(),
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;

    authenticated.assert_redirected_to_identity_provider();
    assert_eq!(gateway.store.pending_login_count(), 1);
}

#[test]
async fn disabled_leaves_the_proxy_headers_in_the_request() {
    let gateway = Gateway::disabled();

    let authenticated = gateway
        .authenticate(
            &open_route(),
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;

    assert!(matches!(authenticated.result, Ok(None)));
    assert_eq!(authenticated.request.headers()[SECRET_HEADER], SECRET);
    assert_eq!(authenticated.request.headers()[IDENTITY_HEADER], IDENTITY);
}

#[test]
async fn disabled_session_from_header_route_needs_no_secret() {
    let gateway = Gateway::disabled();

    gateway
        .authenticate(
            &session_from_header_route(),
            &[(ROUTE_SESSION_HEADER, r#"{"subject":"dev-user"}"#)],
        )
        .await
        .assert_passed_as("dev-user");

    gateway
        .authenticate(
            &session_from_header_route(),
            &[(ROUTE_SESSION_HEADER, "{}")],
        )
        .await
        .assert_passed_as("test-user");
}

#[test]
async fn disabled_session_from_header_route_without_header_is_unauthorized() {
    let gateway = Gateway::disabled();

    let authenticated = gateway
        .authenticate(&session_from_header_route(), &[])
        .await;

    let Ok(Some(response)) = authenticated.result else {
        panic!("expected a response");
    };
    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
}

// --- enabled, OIDC route ---

#[test]
async fn valid_assertion_reaches_the_agent_as_oidc_principal() {
    let gateway = Gateway::enabled();

    let authenticated = gateway
        .authenticate(
            &oidc_route(),
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;

    // no response is produced by the middlewares, so there is no redirect and no Set-Cookie
    assert!(matches!(authenticated.result, Ok(None)));

    let Principal::Oidc(principal) = authenticated.principal() else {
        panic!("expected an OIDC principal");
    };
    assert_eq!(principal.sub, "api-key-user");
    assert_eq!(principal.issuer, "https://keys.example.com");
    assert_eq!(principal.email.as_deref(), Some("robot@example.com"));
    assert_eq!(principal.email_verified, Some(true));
    assert_eq!(principal.name.as_deref(), Some("Robot"));
    assert_eq!(principal.given_name, None);

    let claims: serde_json::Value = serde_json::from_str(&principal.claims).unwrap();
    assert_eq!(claims["sub"], "api-key-user");
    assert_eq!(claims["iss"], "https://keys.example.com");

    // nothing was persisted or even looked up
    assert_eq!(gateway.store.accesses(), 0);
    assert_eq!(gateway.store.session_count(), 0);
    assert_eq!(gateway.store.pending_login_count(), 0);
}

#[test]
async fn wrong_secret_is_unauthorized() {
    let gateway = Gateway::enabled();

    for wrong in [WRONG_SECRET, "", &SECRET[..SECRET.len() - 1]] {
        gateway
            .authenticate(
                &oidc_route(),
                &[(SECRET_HEADER, wrong), (IDENTITY_HEADER, IDENTITY)],
            )
            .await
            .assert_unauthorized()
            .await;
    }

    assert_eq!(gateway.store.accesses(), 0);
}

#[test]
async fn repeated_secret_header_is_unauthorized() {
    let gateway = Gateway::enabled();

    gateway
        .authenticate(
            &oidc_route(),
            &[
                (SECRET_HEADER, SECRET),
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
            ],
        )
        .await
        .assert_unauthorized()
        .await;
}

#[test]
async fn valid_secret_without_identity_is_unauthorized() {
    let gateway = Gateway::enabled();

    gateway
        .authenticate(&oidc_route(), &[(SECRET_HEADER, SECRET)])
        .await
        .assert_unauthorized()
        .await;

    assert_eq!(gateway.store.accesses(), 0);
}

#[test]
async fn incomplete_or_malformed_identity_is_unauthorized() {
    let gateway = Gateway::enabled();

    let expired = format!(
        r#"{{"subject":"api-key-user","issuer":"https://keys.example.com","expires_at":"{}"}}"#,
        (Utc::now() - TimeDelta::seconds(1)).to_rfc3339()
    );

    let rejected_identities = [
        // no subject
        r#"{"issuer":"https://keys.example.com"}"#,
        // no issuer
        r#"{"subject":"api-key-user"}"#,
        // neither: the test identity defaults must not apply
        "{}",
        // blank subject
        r#"{"subject":" ","issuer":"https://keys.example.com"}"#,
        // null subject
        r#"{"subject":null,"issuer":"https://keys.example.com"}"#,
        // issuer is not a URL
        r#"{"subject":"api-key-user","issuer":"keys"}"#,
        // expired
        expired.as_str(),
        // malformed JSON
        r#"{"subject":"api-key-user","#,
        "api-key-user",
        "",
    ];

    for identity in rejected_identities {
        gateway
            .authenticate(
                &oidc_route(),
                &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, identity)],
            )
            .await
            .assert_unauthorized()
            .await;
    }

    assert_eq!(gateway.store.accesses(), 0);
}

#[test]
async fn repeated_identity_header_is_unauthorized() {
    let gateway = Gateway::enabled();

    gateway
        .authenticate(
            &oidc_route(),
            &[
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
                (IDENTITY_HEADER, IDENTITY),
            ],
        )
        .await
        .assert_unauthorized()
        .await;
}

#[test]
async fn unexpired_expires_at_is_honoured() {
    let gateway = Gateway::enabled();
    let expires_at = Utc::now() + TimeDelta::minutes(5);
    let identity = format!(
        r#"{{"subject":"api-key-user","issuer":"https://keys.example.com","expires_at":"{}"}}"#,
        expires_at.to_rfc3339()
    );

    let authenticated = gateway
        .authenticate(
            &oidc_route(),
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, &identity)],
        )
        .await;

    authenticated.assert_passed_as("api-key-user");
    let session_expiry: DateTime<Utc> = authenticated
        .request
        .authenticated_session()
        .unwrap()
        .expires_at;
    assert_eq!(session_expiry.timestamp(), expires_at.timestamp());
}

#[test]
async fn cookie_session_works_without_secret_header() {
    let gateway = Gateway::enabled();
    let cookie = gateway.login("browser-user");

    gateway
        .authenticate(&oidc_route(), &[("cookie", &cookie)])
        .await
        .assert_passed_as("browser-user");
}

#[test]
async fn no_secret_and_no_cookie_redirects_to_identity_provider() {
    let gateway = Gateway::enabled();

    gateway
        .authenticate(&oidc_route(), &[])
        .await
        .assert_redirected_to_identity_provider();

    assert_eq!(gateway.store.pending_login_count(), 1);
}

#[test]
async fn identity_header_without_secret_is_ignored() {
    let gateway = Gateway::enabled();

    let authenticated = gateway
        .authenticate(&oidc_route(), &[(IDENTITY_HEADER, IDENTITY)])
        .await;

    authenticated.assert_redirected_to_identity_provider();
    assert!(
        !authenticated
            .request
            .headers()
            .contains_key(IDENTITY_HEADER)
    );
}

#[test]
async fn assertion_takes_precedence_over_cookie_session() {
    let gateway = Gateway::enabled();
    let cookie = gateway.login("browser-user");

    gateway
        .authenticate(
            &oidc_route(),
            &[
                ("cookie", &cookie),
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
            ],
        )
        .await
        .assert_passed_as("api-key-user");

    assert_eq!(gateway.store.accesses(), 0);
}

#[test]
async fn wrong_secret_does_not_fall_back_to_cookie_session() {
    let gateway = Gateway::enabled();
    let cookie = gateway.login("browser-user");

    gateway
        .authenticate(
            &oidc_route(),
            &[
                ("cookie", &cookie),
                (SECRET_HEADER, WRONG_SECRET),
                (IDENTITY_HEADER, IDENTITY),
            ],
        )
        .await
        .assert_unauthorized()
        .await;

    assert_eq!(gateway.store.accesses(), 0);
}

// --- enabled, session-from-header route ---

const ROUTE_SESSION: &str = r#"{"subject":"client-chosen","issuer":"https://keys.example.com"}"#;

#[test]
async fn session_route_without_secret_is_unauthorized() {
    let gateway = Gateway::enabled();

    for headers in [
        vec![],
        vec![(ROUTE_SESSION_HEADER, ROUTE_SESSION)],
        vec![(IDENTITY_HEADER, IDENTITY)],
        vec![
            (ROUTE_SESSION_HEADER, ROUTE_SESSION),
            (IDENTITY_HEADER, IDENTITY),
        ],
    ] {
        gateway
            .authenticate(&session_from_header_route(), &headers)
            .await
            .assert_unauthorized()
            .await;
    }
}

#[test]
async fn session_route_with_wrong_secret_is_unauthorized() {
    let gateway = Gateway::enabled();

    gateway
        .authenticate(
            &session_from_header_route(),
            &[
                (ROUTE_SESSION_HEADER, ROUTE_SESSION),
                (IDENTITY_HEADER, IDENTITY),
                (SECRET_HEADER, WRONG_SECRET),
            ],
        )
        .await
        .assert_unauthorized()
        .await;
}

#[test]
async fn session_route_takes_the_identity_from_the_identity_header_only() {
    let gateway = Gateway::enabled();
    let route = session_from_header_route();

    let authenticated = gateway
        .authenticate(
            &route,
            &[
                (ROUTE_SESSION_HEADER, ROUTE_SESSION),
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
            ],
        )
        .await;

    authenticated.assert_passed_as("api-key-user");

    // the route's own header is gone: it cannot be bound to an agent parameter either
    for header in [ROUTE_SESSION_HEADER, SECRET_HEADER, IDENTITY_HEADER] {
        assert!(!authenticated.request.headers().contains_key(header));
        assert_eq!(
            CallAgentHandler::resolve_method_arguments(
                &route,
                &authenticated.request,
                &call_agent_behaviour(header, optional_string()),
                ParsedRequestBody::Unused,
            )
            .unwrap(),
            vec![UntypedElementValue::ComponentModel(
                golem_wasm::Value::Option(None)
            )]
        );
    }
}

#[test]
async fn session_route_with_secret_but_only_its_own_header_is_unauthorized() {
    let gateway = Gateway::enabled();

    for route_session in [ROUTE_SESSION, "{}", r#"{"subject":"dev-user"}"#] {
        let authenticated = gateway
            .authenticate(
                &session_from_header_route(),
                &[
                    (ROUTE_SESSION_HEADER, route_session),
                    (SECRET_HEADER, SECRET),
                ],
            )
            .await;

        assert!(
            !authenticated
                .request
                .headers()
                .contains_key(ROUTE_SESSION_HEADER)
        );
        authenticated.assert_unauthorized().await;
    }
}

#[test]
async fn session_route_never_defaults_the_identity() {
    let gateway = Gateway::enabled();

    for identity in [
        "{}",
        r#"{"subject":"dev-user"}"#,
        r#"{"issuer":"https://keys.example.com"}"#,
    ] {
        gateway
            .authenticate(
                &session_from_header_route(),
                &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, identity)],
            )
            .await
            .assert_unauthorized()
            .await;
    }
}

#[test]
async fn session_route_with_repeated_headers_is_unauthorized() {
    let gateway = Gateway::enabled();

    for headers in [
        vec![
            (SECRET_HEADER, SECRET),
            (SECRET_HEADER, SECRET),
            (IDENTITY_HEADER, IDENTITY),
        ],
        vec![
            (SECRET_HEADER, SECRET),
            (IDENTITY_HEADER, IDENTITY),
            (IDENTITY_HEADER, IDENTITY),
        ],
    ] {
        gateway
            .authenticate(&session_from_header_route(), &headers)
            .await
            .assert_unauthorized()
            .await;
    }
}

// --- enabled, identity rules ---

#[test]
async fn issuer_outside_the_allowed_list_is_unauthorized() {
    let gateway = Gateway::enabled();

    for route in [oidc_route(), session_from_header_route()] {
        for issuer in [
            "https://issuer.example",
            "https://keys.example.com/",
            "https://keys.example.com/other",
            "http://keys.example.com",
        ] {
            let identity = format!(r#"{{"subject":"api-key-user","issuer":"{issuer}"}}"#);
            gateway
                .authenticate(
                    &route,
                    &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, &identity)],
                )
                .await
                .assert_unauthorized()
                .await;
        }
    }
}

#[test]
async fn identities_that_could_collide_or_never_expire_are_unauthorized() {
    let gateway = Gateway::enabled();
    let far_future = (Utc::now() + TimeDelta::hours(25)).to_rfc3339();
    let with = |extra: &str| {
        format!(r#"{{"subject":"api-key-user","issuer":"https://keys.example.com",{extra}}}"#)
    };

    let rejected = [
        // separators inside subject or issuer
        r#"{"subject":"api-key\nuser","issuer":"https://keys.example.com"}"#.to_string(),
        r#"{"subject":"api-key-user\t","issuer":"https://keys.example.com"}"#.to_string(),
        r#"{"subject":"api-key-user","issuer":"https://keys.example.com\n"}"#.to_string(),
        r#"{"subject":"api-key-user","issuer":"https://keys.exa\nmple.com"}"#.to_string(),
        // not an http(s) URL
        r#"{"subject":"api-key-user","issuer":"foo:bar"}"#.to_string(),
        // unknown fields
        with(r#""exp":1"#),
        with(r#""expiresAt":"2020-01-01T00:00:00Z""#),
        // numeric and far-future time stamps
        with(r#""expires_at":1893456000"#),
        with(&format!(r#""expires_at":"{far_future}""#)),
        with(&format!(r#""issued_at":"{far_future}""#)),
    ];

    for identity in rejected {
        gateway
            .authenticate(
                &oidc_route(),
                &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, &identity)],
            )
            .await
            .assert_unauthorized()
            .await;
    }

    assert_eq!(gateway.store.accesses(), 0);
}

// --- enabled, route without security ---

#[test]
async fn open_route_is_unaffected_but_headers_are_removed() {
    let gateway = Gateway::enabled();

    for secret in [SECRET, WRONG_SECRET] {
        let authenticated = gateway
            .authenticate(
                &open_route(),
                &[(SECRET_HEADER, secret), (IDENTITY_HEADER, IDENTITY)],
            )
            .await;

        assert!(matches!(authenticated.result, Ok(None)));
        assert!(authenticated.request.authenticated_session().is_none());
        assert_eq!(authenticated.principal(), Principal::anonymous());
        assert!(!authenticated.request.headers().contains_key(SECRET_HEADER));
        assert!(
            !authenticated
                .request
                .headers()
                .contains_key(IDENTITY_HEADER)
        );
    }
}

// --- header stripping ---

#[test]
async fn proxy_headers_cannot_be_bound_to_agent_parameters() {
    let gateway = Gateway::enabled();
    let route = oidc_route();

    let authenticated = gateway
        .authenticate(
            &route,
            &[
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
                ("X-Other", "visible"),
            ],
        )
        .await;
    authenticated.assert_passed_as("api-key-user");

    let bind = |header: &str, parameter_type: QueryOrHeaderType| {
        CallAgentHandler::resolve_method_arguments(
            &route,
            &authenticated.request,
            &call_agent_behaviour(header, parameter_type),
            ParsedRequestBody::Unused,
        )
    };

    for header in [SECRET_HEADER, IDENTITY_HEADER] {
        // an optional header parameter is absent
        assert_eq!(
            bind(header, optional_string()).unwrap(),
            vec![UntypedElementValue::ComponentModel(
                golem_wasm::Value::Option(None)
            )]
        );
        // a required header parameter fails the request
        assert!(matches!(
            bind(header, QueryOrHeaderType::Primitive(PathSegmentType::Str)),
            Err(RequestHandlerError::MissingValue { .. })
        ));
    }

    // other headers stay bindable
    assert_eq!(
        bind("X-Other", optional_string()).unwrap(),
        vec![UntypedElementValue::ComponentModel(
            golem_wasm::Value::Option(Some(Box::new(golem_wasm::Value::String(
                "visible".to_string()
            ))))
        )]
    );
}

#[test]
async fn stripped_request_does_not_reveal_the_credentials() {
    let gateway = Gateway::enabled();

    let authenticated = gateway
        .authenticate(
            &oidc_route(),
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;

    let logged = format!("{:?}", authenticated.request.underlying);
    assert!(!logged.contains(SECRET), "{logged}");
    assert!(!logged.contains("api-key-user"), "{logged}");
    assert!(
        !logged
            .to_lowercase()
            .contains(&SECRET_HEADER.to_lowercase())
    );
}

// --- configuration ---

#[test]
fn secret_is_not_printed() {
    let config = WorkerServiceConfig {
        trusted_identity_proxy: TrustedIdentityProxyConfig {
            previous_secret: Some(TrustedProxySecret::new(WRONG_SECRET)),
            ..enabled_config()
        },
        ..Default::default()
    };

    for printed in [
        format!("{config:?}"),
        format!("{config:#?}"),
        config.to_safe_string(),
        config.to_safe_string_indented(),
    ] {
        assert!(!printed.contains(SECRET), "{printed}");
        assert!(!printed.contains(WRONG_SECRET), "{printed}");
    }
}

/// The section as the service reads it from `GOLEM__TRUSTED_IDENTITY_PROXY__<KEY>` variables:
/// figment infers a type from the text of every environment variable, which is reproduced
/// here by parsing each raw value the same way instead of touching the process environment.
fn config_from_env(vars: &[(&str, &str)]) -> Result<TrustedIdentityProxyConfig, figment::Error> {
    let mut figment = Figment::from(Serialized::defaults(WorkerServiceConfig::default()));
    for (key, raw) in vars {
        let value: figment::value::Value = raw.parse().expect("infallible");
        figment = figment.merge((
            format!("trusted_identity_proxy.{}", key.to_lowercase()),
            value,
        ));
    }
    figment
        .extract::<WorkerServiceConfig>()
        .map(|config| config.trusted_identity_proxy)
}

#[test]
fn whole_section_round_trips_through_environment_values() {
    let config = config_from_env(&[
        ("ENABLED", "true"),
        ("SECRET", SECRET),
        ("PREVIOUS_SECRET", WRONG_SECRET),
        ("SECRET_HEADER", "X-Proxy-Secret"),
        ("IDENTITY_HEADER", "X-Proxy-Identity"),
        (
            "ALLOWED_ISSUERS",
            r#"["https://keys.example.com","https://other.example.com/tenant"]"#,
        ),
    ])
    .unwrap();

    assert!(config.enabled);
    assert_eq!(config.secret.as_ref().unwrap().expose(), SECRET.as_bytes());
    assert_eq!(
        config.previous_secret.as_ref().unwrap().expose(),
        WRONG_SECRET.as_bytes()
    );
    assert_eq!(config.secret_header, "X-Proxy-Secret");
    assert_eq!(config.identity_header, "X-Proxy-Identity");
    assert_eq!(
        config.allowed_issuers,
        vec![
            "https://keys.example.com",
            "https://other.example.com/tenant"
        ]
    );
    assert!(
        TrustedIdentityProxy::from_config(&config)
            .unwrap()
            .is_enabled()
    );
}

#[test]
fn allowed_issuers_environment_forms() {
    let expected = vec![
        "https://keys.example.com",
        "https://other.example.com/tenant",
    ];

    for raw in [
        r#"["https://keys.example.com","https://other.example.com/tenant"]"#,
        r#"["https://keys.example.com", "https://other.example.com/tenant"]"#,
        "https://keys.example.com,https://other.example.com/tenant",
        "https://keys.example.com, https://other.example.com/tenant",
    ] {
        let config = config_from_env(&[("ALLOWED_ISSUERS", raw)]).unwrap();
        assert_eq!(config.allowed_issuers, expected, "{raw}");
    }

    let single = config_from_env(&[("ALLOWED_ISSUERS", "https://keys.example.com")]).unwrap();
    assert_eq!(single.allowed_issuers, vec!["https://keys.example.com"]);
}

#[test]
fn secret_that_reads_as_number_or_boolean_is_refused_without_being_echoed() {
    // What figment reads as something other than a string: integers that fit a machine
    // word, anything with a `.` that parses as a float, and the two booleans.
    for raw in [
        "1234567890123456789",
        "-123456789012345678",
        "123456789012345678901234567890.5",
        "1234567890123456789012345678901234567890.0",
        "true",
        "false",
    ] {
        let error = match config_from_env(&[("ENABLED", "true"), ("SECRET", raw)]) {
            Ok(_) => panic!("{raw} was read as a string"),
            Err(error) => error,
        };
        let message = format!("{error} / {error:?}");
        assert!(message.contains("must be a string"), "{message}");
        if raw.len() > 5 {
            assert!(!message.contains(raw), "{message}");
            assert!(!message.contains(&raw[..6]), "{message}");
        }
    }

    // Longer all-digit values are not inferred as numbers: they stay strings, as do quoted
    // values and everything containing a letter.
    for (raw, secret) in [
        (
            "12345678901234567890123456789012",
            "12345678901234567890123456789012",
        ),
        (
            r#""12345678901234567890123456789012""#,
            "12345678901234567890123456789012",
        ),
        (r#""1234567890.5""#, "1234567890.5"),
        (SECRET, SECRET),
        (
            "K34Co4pRMDl7dB3cYB+1/N0J3tMzJWI4FyPNOsmGHkE=",
            "K34Co4pRMDl7dB3cYB+1/N0J3tMzJWI4FyPNOsmGHkE=",
        ),
    ] {
        let config = config_from_env(&[("SECRET", raw)]).unwrap();
        assert_eq!(config.secret.unwrap().expose(), secret.as_bytes(), "{raw}");
    }
}
