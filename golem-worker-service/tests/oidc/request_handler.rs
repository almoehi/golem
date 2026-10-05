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

//! Drives the real `RequestHandler` — route resolution, security middlewares, parameter
//! binding and agent invocation — with only the registry lookup, the authorization and the
//! worker executor client replaced.

use crate::handler::FakeIdentityProvider;
use crate::trusted_identity_proxy::{
    IDENTITY, IDENTITY_HEADER, RecordingSessionStore, SECRET, SECRET_HEADER, WRONG_SECRET,
    enabled_config,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use golem_api_grpc::proto::golem::worker::{InvocationContext, LogEvent};
use golem_common::model::account::AccountId;
use golem_common::model::agent::{
    AgentTypeName, DataSchema, HttpMethod, NamedElementSchemas, Principal, UntypedDataValue,
    UntypedElementValue,
};
use golem_common::model::auth::TokenSecret;
use golem_common::model::component::{
    CanonicalFilePath, ComponentId, ComponentRevision, PluginPriority,
};
use golem_common::model::deployment::DeploymentRevision;
use golem_common::model::domain_registration::Domain;
use golem_common::model::environment::EnvironmentId;
use golem_common::model::oplog::{OplogCursor, OplogIndex};
use golem_common::model::security_scheme::{Provider, SecuritySchemeId, SecuritySchemeName};
use golem_common::model::worker::{
    AgentConfigEntryDto, AgentMetadataDto, AgentUpdateMode, RevertWorkerTarget,
};
use golem_common::model::{
    AgentFilter, AgentFingerprint, AgentId, AgentInvocationOutput, AgentInvocationResult, Empty,
    IdempotencyKey, ScanCursor,
};
use golem_service_base::clients::registry::{
    GrpcRegistryService, GrpcRegistryServiceConfig, RegistryService,
};
use golem_service_base::custom_api::{
    CallAgentBehaviour, CompiledRoute, CompiledRoutes, CorsOptions, CorsPreflightBehaviour,
    CorsPreflightMethodPolicy, MethodParameter, OriginPattern, PathSegment, PathSegmentType,
    QueryOrHeaderType, RequestBodySchema, RouteBehaviour, RouteSecurity, SecuritySchemeDetails,
    SecuritySchemeRouteSecurity, SessionFromHeaderRouteSecurity,
};
use golem_service_base::model::auth::{AuthCtx, AuthDetailsForEnvironment, EnvironmentAction};
use golem_service_base::model::{ComponentFileSystemNode, GetOplogResponse};
use golem_worker_service::api::common::ApiEndpointError;
use golem_worker_service::config::{
    ComponentServiceConfig, RouteResolverConfig, TrustedIdentityProxyConfig,
};
use golem_worker_service::custom_api::api_definition_lookup::{
    ApiDefinitionLookupError, HttpApiDefinitionsLookup,
};
use golem_worker_service::custom_api::call_agent::CallAgentHandler;
use golem_worker_service::custom_api::error::RequestHandlerError;
use golem_worker_service::custom_api::oidc::handler::OidcHandler;
use golem_worker_service::custom_api::request_handler::RequestHandler;
use golem_worker_service::custom_api::route_resolver::RouteResolver;
use golem_worker_service::custom_api::trusted_identity_proxy::TrustedIdentityProxy;
use golem_worker_service::custom_api::webhooks::WebhookCallbackHandler;
use golem_worker_service::service::agent_resolution_cache::AgentResolutionCache;
use golem_worker_service::service::auth::{AuthService, AuthServiceError};
use golem_worker_service::service::component::RemoteComponentService;
use golem_worker_service::service::limit::RemoteLimitService;
use golem_worker_service::service::worker::{
    WorkerClient, WorkerResult, WorkerService, WorkerServiceError, WorkerStream,
};
use http::StatusCode;
use openidconnect::{ClientId, ClientSecret, RedirectUrl, Scope};
use poem::IntoResponse;
use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use test_r::test;
use tracing::instrument::WithSubscriber;

const HOST: &str = "example.com";
const ROUTE_SESSION_HEADER: &str = "X-Test-Session";

/// What the gateway handed to the worker executor for one agent invocation
#[derive(Debug, Clone)]
struct Invocation {
    method_parameters: UntypedDataValue,
    principal: Principal,
}

/// Worker executor client that records agent invocations instead of executing them
#[derive(Default)]
struct RecordingWorkerClient {
    invocations: Mutex<Vec<Invocation>>,
}

#[async_trait]
impl WorkerClient for RecordingWorkerClient {
    async fn invoke_agent(
        &self,
        _agent_id: &AgentId,
        _method_name: Option<String>,
        method_parameters: Option<golem_api_grpc::proto::golem::component::UntypedDataValue>,
        _mode: i32,
        _schedule_at: Option<::prost_types::Timestamp>,
        _idempotency_key: IdempotencyKey,
        _invocation_context: Option<InvocationContext>,
        _environment_id: EnvironmentId,
        _account_id: AccountId,
        _auth_ctx: AuthCtx,
        principal: golem_api_grpc::proto::golem::component::Principal,
    ) -> WorkerResult<AgentInvocationOutput> {
        self.invocations.lock().unwrap().push(Invocation {
            method_parameters: method_parameters
                .expect("method parameters")
                .try_into()
                .expect("valid method parameters"),
            principal: principal.try_into().expect("valid principal"),
        });

        Ok(AgentInvocationOutput {
            result: AgentInvocationResult::AgentMethod {
                output: UntypedDataValue::Tuple(vec![]),
            },
            consumed_fuel: None,
            invocation_status: None,
            component_revision: None,
        })
    }

    async fn create(
        &self,
        _agent_id: &AgentId,
        _environment_variables: HashMap<String, String>,
        _config: Vec<AgentConfigEntryDto>,
        _ignore_already_existing: bool,
        _account_id: AccountId,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
        _invocation_context: Option<InvocationContext>,
        _principal: Option<golem_api_grpc::proto::golem::component::Principal>,
    ) -> WorkerResult<(AgentId, AgentFingerprint)> {
        unimplemented!("only agent invocations are expected")
    }

    async fn connect(
        &self,
        _agent_id: &AgentId,
        _environment_id: EnvironmentId,
        _account_id: AccountId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<WorkerStream<LogEvent>> {
        unimplemented!("only agent invocations are expected")
    }

    async fn delete(
        &self,
        _agent_id: &AgentId,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn complete_promise(
        &self,
        _agent_id: &AgentId,
        _oplog_id: u64,
        _data: Vec<u8>,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<bool> {
        unimplemented!("only agent invocations are expected")
    }

    async fn interrupt(
        &self,
        _agent_id: &AgentId,
        _recover_immediately: bool,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn get_metadata(
        &self,
        _agent_id: &AgentId,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<AgentMetadataDto> {
        unimplemented!("only agent invocations are expected")
    }

    async fn find_metadata(
        &self,
        _component_id: ComponentId,
        _filter: Option<AgentFilter>,
        _cursor: ScanCursor,
        _count: u64,
        _precise: bool,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<(Option<ScanCursor>, Vec<AgentMetadataDto>)> {
        unimplemented!("only agent invocations are expected")
    }

    async fn resume(
        &self,
        _agent_id: &AgentId,
        _force: bool,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn update(
        &self,
        _agent_id: &AgentId,
        _update_mode: AgentUpdateMode,
        _target_revision: ComponentRevision,
        _disable_wakeup: bool,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn get_oplog(
        &self,
        _agent_id: &AgentId,
        _from_oplog_index: OplogIndex,
        _cursor: Option<OplogCursor>,
        _count: u64,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> Result<GetOplogResponse, WorkerServiceError> {
        unimplemented!("only agent invocations are expected")
    }

    async fn search_oplog(
        &self,
        _agent_id: &AgentId,
        _cursor: Option<OplogCursor>,
        _count: u64,
        _query: String,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> Result<GetOplogResponse, WorkerServiceError> {
        unimplemented!("only agent invocations are expected")
    }

    async fn get_file_system_node(
        &self,
        _agent_id: &AgentId,
        _path: CanonicalFilePath,
        _environment_id: EnvironmentId,
        _account_id: AccountId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<Vec<ComponentFileSystemNode>> {
        unimplemented!("only agent invocations are expected")
    }

    async fn get_file_contents(
        &self,
        _agent_id: &AgentId,
        _path: CanonicalFilePath,
        _environment_id: EnvironmentId,
        _account_id: AccountId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<Pin<Box<dyn Stream<Item = WorkerResult<Bytes>> + Send + 'static>>> {
        unimplemented!("only agent invocations are expected")
    }

    async fn activate_plugin(
        &self,
        _agent_id: &AgentId,
        _plugin_priority: PluginPriority,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn deactivate_plugin(
        &self,
        _agent_id: &AgentId,
        _plugin_priority: PluginPriority,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn fork_worker(
        &self,
        _source_agent_id: &AgentId,
        _target_agent_id: &AgentId,
        _oplog_index_cut_off: OplogIndex,
        _environment_id: EnvironmentId,
        _account_id: AccountId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn revert_worker(
        &self,
        _agent_id: &AgentId,
        _target: RevertWorkerTarget,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }

    async fn cancel_invocation(
        &self,
        _agent_id: &AgentId,
        _idempotency_key: &IdempotencyKey,
        _environment_id: EnvironmentId,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<bool> {
        unimplemented!("only agent invocations are expected")
    }

    async fn process_oplog_entries(
        &self,
        _target_agent_id: &AgentId,
        _environment_id: EnvironmentId,
        _component_revision: ComponentRevision,
        _idempotency_key: IdempotencyKey,
        _account_id: AccountId,
        _config: HashMap<String, String>,
        _metadata: golem_api_grpc::proto::golem::worker::AgentMetadata,
        _first_entry_index: OplogIndex,
        _entries: Vec<golem_api_grpc::proto::golem::worker::RawOplogEntry>,
        _auth_ctx: AuthCtx,
    ) -> WorkerResult<()> {
        unimplemented!("only agent invocations are expected")
    }
}

/// Authorizes every environment action
struct AllowAllAuthService;

#[async_trait]
impl AuthService for AllowAllAuthService {
    async fn authenticate_token(&self, _token: TokenSecret) -> Result<AuthCtx, AuthServiceError> {
        unimplemented!("the custom API does not authenticate tokens")
    }

    async fn authorize_environment_actions(
        &self,
        _environment_id: EnvironmentId,
        _action: EnvironmentAction,
        _auth_ctx: &AuthCtx,
    ) -> Result<AuthDetailsForEnvironment, AuthServiceError> {
        Ok(AuthDetailsForEnvironment {
            account_id_owning_environment: AccountId::default(),
            environment_roles_from_shares: BTreeSet::new(),
        })
    }
}

/// The deployed HTTP API of `HOST`: endpoints that bind the trusted identity proxy headers
/// as agent parameters behind each kind of route security, plus a CORS pre-flight route. The
/// OIDC callback route (`GET /redirect`) is added by the route resolver from the scheme.
struct StaticApiDefinition {
    security_scheme_id: SecuritySchemeId,
}

impl StaticApiDefinition {
    fn bound_headers_route(route_id: i32, path: &str, security: RouteSecurity) -> CompiledRoute {
        let optional_header = |header_name: &str| MethodParameter::Header {
            header_name: header_name.to_string(),
            parameter_type: QueryOrHeaderType::Option {
                name: None,
                owner: None,
                inner: Box::new(PathSegmentType::Str),
            },
        };

        CompiledRoute {
            route_id,
            method: HttpMethod::Get(Empty {}),
            path: vec![PathSegment::Literal {
                value: path.to_string(),
            }],
            body: RequestBodySchema::Unused,
            behavior: RouteBehaviour::CallAgent(CallAgentBehaviour {
                component_id: ComponentId::new(),
                component_revision: ComponentRevision::INITIAL,
                agent_type: AgentTypeName("test-agent".to_string()),
                constructor_parameters: vec![],
                phantom: false,
                method_name: "headers".to_string(),
                method_parameters: vec![
                    optional_header(SECRET_HEADER),
                    optional_header(IDENTITY_HEADER),
                    optional_header(ROUTE_SESSION_HEADER),
                    optional_header("X-Other"),
                ],
                expected_agent_response: DataSchema::Tuple(NamedElementSchemas {
                    elements: vec![],
                }),
                method_description: None,
            }),
            security,
            cors: CorsOptions {
                allowed_patterns: vec![],
            },
        }
    }
}

#[async_trait]
impl HttpApiDefinitionsLookup for StaticApiDefinition {
    async fn get(&self, domain: &Domain) -> Result<CompiledRoutes, ApiDefinitionLookupError> {
        if domain.0 != HOST {
            return Err(ApiDefinitionLookupError::UnknownSite(domain.clone()));
        }

        let security_scheme = SecuritySchemeDetails {
            id: self.security_scheme_id,
            name: SecuritySchemeName("test-scheme".to_string()),
            provider_type: Provider::Google(Empty {}),
            client_id: ClientId::new("client-id".to_string()),
            client_secret: ClientSecret::new("client-secret".to_string()),
            redirect_url: RedirectUrl::new(format!("http://{HOST}/redirect")).unwrap(),
            scopes: vec![Scope::new("openid".to_string())],
        };

        Ok(CompiledRoutes {
            account_id: AccountId::default(),
            environment_id: EnvironmentId::default(),
            deployment_revision: DeploymentRevision::INITIAL,
            security_schemes: HashMap::from([(self.security_scheme_id, security_scheme)]),
            routes: vec![
                Self::bound_headers_route(
                    1,
                    "oidc",
                    RouteSecurity::SecurityScheme(SecuritySchemeRouteSecurity {
                        security_scheme_id: self.security_scheme_id,
                    }),
                ),
                Self::bound_headers_route(
                    2,
                    "session",
                    RouteSecurity::SessionFromHeader(SessionFromHeaderRouteSecurity {
                        header_name: ROUTE_SESSION_HEADER.to_string(),
                    }),
                ),
                Self::bound_headers_route(3, "open", RouteSecurity::None),
                CompiledRoute {
                    route_id: 4,
                    method: HttpMethod::Options(Empty {}),
                    path: vec![PathSegment::Literal {
                        value: "oidc".to_string(),
                    }],
                    body: RequestBodySchema::Unused,
                    behavior: RouteBehaviour::CorsPreflight(CorsPreflightBehaviour {
                        method_policies: vec![CorsPreflightMethodPolicy {
                            method: HttpMethod::Get(Empty {}),
                            allowed_origins: BTreeSet::from([OriginPattern(
                                "http://app.example.com".to_string(),
                            )]),
                            allowed_headers: BTreeSet::new(),
                        }],
                    }),
                    security: RouteSecurity::None,
                    cors: CorsOptions {
                        allowed_patterns: vec![],
                    },
                },
            ],
        })
    }
}

/// Everything written to the log while a request is handled, at the most verbose level
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl CapturedLog {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(self.clone())
            .finish()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct TestGateway {
    request_handler: RequestHandler,
    worker_client: Arc<RecordingWorkerClient>,
    session_store: Arc<RecordingSessionStore>,
}

/// One handled request: the response status (or the error status), and the log it produced
struct Handled {
    status: StatusCode,
    log: String,
}

impl TestGateway {
    fn new(config: &TrustedIdentityProxyConfig) -> Self {
        // Never contacted: route resolution and authorization are replaced, and the gateway
        // passes the environment of the route along with the invocation.
        let registry: Arc<dyn RegistryService> = Arc::new(GrpcRegistryService::new(
            &GrpcRegistryServiceConfig::default(),
        ));

        let worker_client = Arc::new(RecordingWorkerClient::default());
        let worker_service = Arc::new(WorkerService::new(
            Arc::new(RemoteComponentService::new(
                registry.clone(),
                &ComponentServiceConfig::default(),
            )),
            Arc::new(AllowAllAuthService),
            Arc::new(RemoteLimitService::new(registry.clone())),
            worker_client.clone(),
            Arc::new(AgentResolutionCache::new(
                registry,
                16,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )),
        ));

        let session_store = Arc::new(RecordingSessionStore::default());

        let request_handler = RequestHandler::new(
            Arc::new(RouteResolver::new(
                &RouteResolverConfig::default(),
                Arc::new(StaticApiDefinition {
                    security_scheme_id: SecuritySchemeId::new(),
                }),
            )),
            Arc::new(CallAgentHandler::new(worker_service.clone())),
            Arc::new(OidcHandler::new(
                session_store.clone(),
                Arc::new(FakeIdentityProvider),
            )),
            Arc::new(WebhookCallbackHandler::new(worker_service, vec![])),
            Arc::new(TrustedIdentityProxy::from_config(config).expect("valid configuration")),
        );

        Self {
            request_handler,
            worker_client,
            session_store,
        }
    }

    async fn handle(&self, method: http::Method, uri: &str, headers: &[(&str, &str)]) -> Handled {
        let mut request = poem::Request::builder()
            .method(method)
            .uri_str(uri)
            .header("host", HOST);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }

        let log = CapturedLog::default();
        let result = self
            .request_handler
            .handle_request(request.finish())
            .with_subscriber(log.subscriber())
            .await;

        let status = match result {
            Ok(response) => response.status(),
            Err(error) => {
                assert!(
                    !matches!(error, RequestHandlerError::InternalError(_)),
                    "{error:?}"
                );
                ApiEndpointError::from(error).into_response().status()
            }
        };

        Handled {
            status,
            log: log.text(),
        }
    }

    async fn get(&self, uri: &str, headers: &[(&str, &str)]) -> Handled {
        self.handle(http::Method::GET, uri, headers).await
    }

    fn invocations(&self) -> Vec<Invocation> {
        self.worker_client.invocations.lock().unwrap().clone()
    }
}

impl Handled {
    /// The request was logged while it was handled, so an absent value is a real absence
    fn assert_logged_without(&self, sensitive: &[&str]) {
        assert!(
            self.log.contains("Begin http request handling"),
            "the request log line was not captured:\n{}",
            self.log
        );
        for value in sensitive {
            assert!(
                !self.log.contains(value),
                "the log contains {value}:\n{}",
                self.log
            );
        }
    }
}

fn absent() -> UntypedElementValue {
    UntypedElementValue::ComponentModel(golem_wasm::Value::Option(None))
}

fn present(value: &str) -> UntypedElementValue {
    UntypedElementValue::ComponentModel(golem_wasm::Value::Option(Some(Box::new(
        golem_wasm::Value::String(value.to_string()),
    ))))
}

/// The four header parameters of the test endpoints as the agent received them:
/// secret header, identity header, the session route's own header, `X-Other`
fn bound_headers(invocation: &Invocation) -> Vec<UntypedElementValue> {
    let UntypedDataValue::Tuple(elements) = &invocation.method_parameters else {
        panic!("expected a tuple of parameters");
    };
    elements.clone()
}

fn subject(invocation: &Invocation) -> Option<String> {
    match &invocation.principal {
        Principal::Oidc(principal) => Some(principal.sub.clone()),
        _ => None,
    }
}

#[test]
async fn proxy_headers_never_reach_the_agent_or_the_log() {
    let gateway = TestGateway::new(&enabled_config());

    let handled = gateway
        .get(
            "/oidc",
            &[
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
                ("X-Other", "visible"),
            ],
        )
        .await;

    assert!(handled.status.is_success(), "{}", handled.status);

    let invocations = gateway.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        bound_headers(&invocations[0]),
        vec![absent(), absent(), absent(), present("visible")]
    );
    assert_eq!(subject(&invocations[0]).as_deref(), Some("api-key-user"));

    // the identity is logged as the principal of the invocation, but never as the header
    handled.assert_logged_without(&[SECRET, IDENTITY, r#"{"subject""#, "x-golem-trusted"]);
    assert_eq!(gateway.session_store.accesses(), 0);
}

#[test]
async fn rejected_secret_never_reaches_the_agent_or_the_log() {
    let gateway = TestGateway::new(&enabled_config());

    for uri in ["/oidc", "/session"] {
        let handled = gateway
            .get(
                uri,
                &[(SECRET_HEADER, WRONG_SECRET), (IDENTITY_HEADER, IDENTITY)],
            )
            .await;

        assert_eq!(handled.status, StatusCode::UNAUTHORIZED);
        handled.assert_logged_without(&[WRONG_SECRET, IDENTITY, "x-golem-trusted"]);
    }

    assert!(gateway.invocations().is_empty());
    assert_eq!(gateway.session_store.accesses(), 0);
}

#[test]
async fn session_route_binds_neither_the_proxy_headers_nor_its_own_header() {
    let gateway = TestGateway::new(&enabled_config());

    let handled = gateway
        .get(
            "/session",
            &[
                (SECRET_HEADER, SECRET),
                (IDENTITY_HEADER, IDENTITY),
                (
                    ROUTE_SESSION_HEADER,
                    r#"{"subject":"client-chosen","issuer":"https://keys.example.com"}"#,
                ),
                ("X-Other", "visible"),
            ],
        )
        .await;

    assert!(handled.status.is_success(), "{}", handled.status);

    let invocations = gateway.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        bound_headers(&invocations[0]),
        vec![absent(), absent(), absent(), present("visible")]
    );
    assert_eq!(subject(&invocations[0]).as_deref(), Some("api-key-user"));
    handled.assert_logged_without(&[SECRET, IDENTITY]);

    // its own header alone is not an identity, with or without the secret
    for headers in [
        vec![(ROUTE_SESSION_HEADER, r#"{"subject":"client-chosen"}"#)],
        vec![
            (ROUTE_SESSION_HEADER, r#"{"subject":"client-chosen"}"#),
            (SECRET_HEADER, SECRET),
        ],
    ] {
        let handled = gateway.get("/session", &headers).await;
        assert_eq!(handled.status, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(gateway.invocations().len(), 1);
}

#[test]
async fn open_route_binds_neither_proxy_header() {
    let gateway = TestGateway::new(&enabled_config());

    for secret in [SECRET, WRONG_SECRET] {
        let handled = gateway
            .get(
                "/open",
                &[
                    (SECRET_HEADER, secret),
                    (IDENTITY_HEADER, IDENTITY),
                    ("X-Other", "visible"),
                ],
            )
            .await;

        assert!(handled.status.is_success(), "{}", handled.status);
        handled.assert_logged_without(&[secret, IDENTITY]);
    }

    for invocation in gateway.invocations() {
        assert_eq!(
            bound_headers(&invocation),
            vec![absent(), absent(), absent(), present("visible")]
        );
        assert_eq!(invocation.principal, Principal::anonymous());
    }
    assert_eq!(gateway.invocations().len(), 2);
}

#[test]
async fn disabled_gateway_passes_the_headers_on_as_before() {
    let gateway = TestGateway::new(&TrustedIdentityProxyConfig::default());

    let handled = gateway
        .get(
            "/open",
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;
    assert!(handled.status.is_success(), "{}", handled.status);
    assert_eq!(
        bound_headers(&gateway.invocations()[0]),
        vec![present(SECRET), present(IDENTITY), absent(), absent()]
    );

    // OIDC route: both headers are meaningless, the login flow starts
    let handled = gateway
        .get(
            "/oidc",
            &[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
        )
        .await;
    assert_eq!(handled.status, StatusCode::FOUND);

    // session route: its own header is trusted verbatim, defaults included
    let handled = gateway
        .get("/session", &[(ROUTE_SESSION_HEADER, "{}")])
        .await;
    assert!(handled.status.is_success(), "{}", handled.status);
    let invocations = gateway.invocations();
    assert_eq!(subject(&invocations[1]).as_deref(), Some("test-user"));
    assert_eq!(
        bound_headers(&invocations[1]),
        vec![absent(), absent(), present("{}"), absent()]
    );
}

#[test]
async fn cors_preflight_is_unchanged_by_the_proxy_headers() {
    let preflight = [
        ("origin", "http://app.example.com"),
        ("access-control-request-method", "GET"),
    ];
    let with = |extra: &[(&'static str, &'static str)]| [&preflight[..], extra].concat();

    let mut statuses = Vec::new();
    for config in [TrustedIdentityProxyConfig::default(), enabled_config()] {
        let gateway = TestGateway::new(&config);
        for headers in [
            with(&[]),
            with(&[(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)]),
            with(&[(SECRET_HEADER, WRONG_SECRET)]),
        ] {
            let handled = gateway
                .handle(http::Method::OPTIONS, "/oidc", &headers)
                .await;
            statuses.push(handled.status);
        }
        assert!(gateway.invocations().is_empty());
        assert_eq!(gateway.session_store.accesses(), 0);
    }

    assert_eq!(statuses, vec![StatusCode::NO_CONTENT; 6]);
}

#[test]
async fn oidc_callback_is_unchanged_by_the_proxy_headers() {
    let mut statuses = Vec::new();
    for config in [TrustedIdentityProxyConfig::default(), enabled_config()] {
        let gateway = TestGateway::new(&config);
        for headers in [
            vec![],
            vec![(SECRET_HEADER, SECRET), (IDENTITY_HEADER, IDENTITY)],
            vec![(SECRET_HEADER, WRONG_SECRET)],
        ] {
            let handled = gateway
                .get("/redirect?code=code&state=unknown-state", &headers)
                .await;
            statuses.push(handled.status);
        }
        assert!(gateway.invocations().is_empty());
    }

    // an unknown login state is refused the same way in every case
    assert_eq!(statuses, vec![StatusCode::FORBIDDEN; 6]);
}
