# Trusted identity proxy

The custom request gateway of the worker service (`custom_request_port`) can accept caller
identities asserted by a reverse proxy in front of it. This is meant for programmatic clients:
the proxy authenticates them (for example with an API key), maps them to a fixed identity and
tells the gateway who is calling. The gateway believes such an assertion only from a caller that
presents a shared secret.

The feature is disabled by default. While it is disabled the gateway behaves exactly as before,
and neither header described below has any meaning.

It only affects the custom request API. MCP, the management REST API and gRPC are unchanged.

## Configuration

Worker service (`config/worker-service.toml` or environment):

| TOML key (`[trusted_identity_proxy]`) | Environment variable | Default |
| --- | --- | --- |
| `enabled` | `GOLEM__TRUSTED_IDENTITY_PROXY__ENABLED` | `false` |
| `secret` | `GOLEM__TRUSTED_IDENTITY_PROXY__SECRET` | not set |
| `previous_secret` | `GOLEM__TRUSTED_IDENTITY_PROXY__PREVIOUS_SECRET` | not set |
| `secret_header` | `GOLEM__TRUSTED_IDENTITY_PROXY__SECRET_HEADER` | `X-Golem-Trusted-Proxy-Secret` |
| `identity_header` | `GOLEM__TRUSTED_IDENTITY_PROXY__IDENTITY_HEADER` | `X-Golem-Trusted-Identity` |
| `allowed_issuers` | `GOLEM__TRUSTED_IDENTITY_PROXY__ALLOWED_ISSUERS` | `[]` |

Single binary (`golem server run`), read from the process environment and mapped one to one
onto the settings above:

| Environment variable | Setting |
| --- | --- |
| `GOLEM_TRUSTED_IDENTITY_PROXY_ENABLED` | `enabled` (`true` or `false`) |
| `GOLEM_TRUSTED_IDENTITY_PROXY_SECRET` | `secret` |
| `GOLEM_TRUSTED_IDENTITY_PROXY_PREVIOUS_SECRET` | `previous_secret` |
| `GOLEM_TRUSTED_IDENTITY_PROXY_SECRET_HEADER` | `secret_header` |
| `GOLEM_TRUSTED_IDENTITY_PROXY_IDENTITY_HEADER` | `identity_header` |
| `GOLEM_TRUSTED_IDENTITY_PROXY_ALLOWED_ISSUERS` | `allowed_issuers`, comma separated |

### One rule for both binaries

The feature is on exactly when `enabled` is `true`. Every other setting of the section requires
it. The service refuses to start when:

- `enabled` is not `true` and any of `secret`, `previous_secret`, `allowed_issuers` is set or a
  header name differs from its default. A secret can therefore never sit in a service that
  neither checks nor removes it. A secret or header name variable that is present but empty
  counts as set.
- `enabled` is `true` and
  - `secret` is missing or shorter than 32 bytes (`previous_secret`: shorter than 32 bytes);
  - `allowed_issuers` is empty, or an entry is not a usable issuer (see "Identity header");
  - a header name is not a valid header name, the two are the same header, or one of them is
    `authorization`, `cookie`, `host`, `origin`, `x-forwarded-host`, `x-forwarded-proto`,
    `x-forwarded-for`, `traceparent`, `tracestate`, `baggage`, `content-length` or
    `content-type`.

### Writing the values

- **Secret**: generate it from a CSPRNG and use a form that contains letters, for example
  `openssl rand -hex 32` or `openssl rand -base64 48`. Environment values are type-inferred by
  the configuration loader: a value that is `true`/`false`, an integer that fits a machine word
  or a number with a `.` is not a string and is refused at startup (the error does not print
  the value). Quote such a value (`SECRET='"1234.5"'`) or, better, avoid it. Surrounding
  whitespace of an unquoted environment value is dropped.
- **Allowed issuers** as an environment variable, either form:

  ```
  GOLEM__TRUSTED_IDENTITY_PROXY__ALLOWED_ISSUERS='["https://keys.example.com","https://other.example.com/tenant"]'
  GOLEM__TRUSTED_IDENTITY_PROXY__ALLOWED_ISSUERS=https://keys.example.com,https://other.example.com/tenant
  ```

  In TOML: `allowed_issuers = ["https://keys.example.com"]`.
- The secret is never logged: the startup configuration log and `Debug` output show `*******`.
  The explicit `--dump-config*` flags print loaded values verbatim, as they do for every other
  secret of the service.

## Behaviour when enabled

Both headers are removed from every incoming request as the first step of handling it, before
it is logged, routed or bound to agent parameters. An agent can never read them (not through
a header parameter either) and they never reach an oplog. They are not echoed in responses or
error messages.

| Route security | Secret header | Result |
| --- | --- | --- |
| Security scheme (OIDC) | absent | Unchanged: session cookie, otherwise `302` to the identity provider |
| Security scheme (OIDC) | valid, with a valid identity header | The agent is invoked with the asserted identity. The cookie is ignored, no session is stored, no `Set-Cookie` is sent |
| Security scheme (OIDC) | valid, identity header missing or invalid | `401` |
| Security scheme (OIDC) | not valid | `401`, never a redirect and never a fallback to the cookie |
| Test session header (`testSessionHeaderName`) | valid, with a valid identity header | The agent is invoked with the asserted identity |
| Test session header (`testSessionHeaderName`) | valid, identity header missing or invalid | `401` |
| Test session header (`testSessionHeaderName`) | absent or not valid | `401` |
| None | any | Unchanged (the headers are still removed) |

On test session header routes the route's own session header is **removed and ignored**: the
identity comes only from the identity header, under the same strict rules as on OIDC routes
(no `test-user` default). Such a route is reachable only through the proxy once the feature is
enabled.

A `401` has a JSON body `{"code":"AUTH_UNAUTHORIZED","error":"..."}`. A malformed identity is a
`401` as well, not a `400`.

A secret header that occurs more than once is not valid; neither is an identity header that
occurs more than once. Secrets are compared in constant time (SHA-256 digests compared with
`subtle`), independent of the length of the presented value.

CORS pre-flight routes and the OIDC callback route have no route security and behave as
before, whatever the two headers contain.

### Identity header

The identity header carries one JSON object:

```json
{
  "subject": "api-key-7f3a",
  "issuer": "https://keys.example.com",
  "email": "robot@example.com",
  "email_verified": true,
  "name": "Robot"
}
```

- `subject` is required. It must not be empty, start or end with whitespace, or contain control
  characters (line breaks and tabs included).
- `issuer` is required, under the same rules, and must
  - be an `http` or `https` URL written exactly as it parses: lower-case scheme and host, no
    default port, no `.`/`..` segments (the one tolerated difference is a missing `/` after
    the host, so `https://keys.example.com` is fine);
  - be one of `allowed_issuers`, compared as an exact string (`https://keys.example.com` and
    `https://keys.example.com/` are different issuers).
- Optional: `email`, `email_verified`, `name`, `given_name`, `family_name`, `picture`,
  `preferred_username`, `scopes`, `issued_at`, `expires_at` (both RFC 3339 strings).
- Any other field is an error. `exp`, `expiresAt` or a numeric `expires_at` give a `401`, not
  an assertion that silently never expires.
- `expires_at` must lie in the future and at most 24 hours ahead; `issued_at` at most 5 minutes
  ahead. Without `expires_at` the assertion is good for this request; nothing is remembered
  between requests either way.

The agent receives an ordinary OIDC principal (`sub`, `issuer`, `email`, ... and `claims`
synthesised from subject and issuer). It cannot tell an asserted identity from an interactive
login. Applications commonly key users by issuer and subject, which is why both are restricted
as above and why the issuers are an allowlist: **never put the issuer of an identity provider
used for logins into `allowed_issuers`** — that would let the proxy (and whoever controls what
it asserts) act as any interactive user.

The identity provider of the security scheme is not contacted for an asserted identity.

## Threat model

- Whoever knows the secret can act as any subject of every allowed issuer on every OIDC and
  test session header route of **every domain and deployment** served by this worker service:
  the secret is not bound to a domain. The gateway picks the deployment from `X-Forwarded-Host`
  or `Host`, so the proxy must set both itself and never forward client supplied values.
  (An allowlist of trusted ingress addresses, as proposed upstream in golem PR #3880, would be
  the complement to this feature; it is not part of it.)
- The proxy must be the only party that knows the secret, and the only network path over which
  the secret can reach the gateway.
- The proxy must remove client supplied copies of both headers from every request before adding
  its own. If it forwards a client supplied secret header next to its own, the request is
  answered with `401`; if it forwards a client supplied identity header next to its own, the
  request is answered with `401` too (repeated header) — but a proxy that *replaces* neither
  and adds only the secret lets the client choose its identity.
- Requests that do not go through the proxy (browsers using the OIDC login) are unaffected and
  cannot assert an identity without the secret. Isolate the gateway port on the network level
  anyway (for example a Kubernetes NetworkPolicy that only admits the proxy and the ingress):
  the secret is the only thing standing between a caller that reaches the port and every
  allowed identity.
- The secret travels in a request header. Use TLS or a trusted network segment between the
  proxy and the gateway.
- The gateway does not rate limit failed attempts. A 32 byte random secret cannot be guessed,
  but put rate limiting in front of the gateway if the port is reachable by untrusted callers.

## Header name collisions

The two header names are removed from every request. If one of them is also

- the `testSessionHeaderName` of a route, or
- a header an endpoint binds to an agent parameter,

that route or endpoint never sees the header: the request fails closed at request time (`401`
for the route security, a missing-value error for a required parameter, an absent value for an
optional one). Nothing detects this at deployment time, so choose header names no application
uses. The route's own session header is logged with the request at debug level before it is
removed; it carries no credential.

## Rotation

Without downtime:

1. Restart the worker service with `secret` = new secret and `previous_secret` = old secret.
2. Switch the proxy to the new secret.
3. Restart the worker service without `previous_secret`.

Or restart the worker service and the proxy together with a new `secret`; requests carrying
the old secret are answered with `401` in between.
