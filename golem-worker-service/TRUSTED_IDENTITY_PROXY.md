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

When `enabled` is true the service refuses to start unless `secret` is set and at least 32 bytes
long (the same holds for `previous_secret` if set), both header names are valid and they differ.
Generate the secret from a CSPRNG, for example `openssl rand -base64 48`.

The secret is never logged: the startup configuration log and `Debug` output show `*******`.
The explicit `--dump-config*` flags print loaded values verbatim, as they do for every other
secret of the service.

Single binary (`golem server run`), read from the process environment:

| Environment variable | Meaning |
| --- | --- |
| `GOLEM_TRUSTED_IDENTITY_PROXY_SECRET` | Setting it (non-empty) enables the feature |
| `GOLEM_TRUSTED_IDENTITY_PROXY_PREVIOUS_SECRET` | Optional, see rotation |
| `GOLEM_TRUSTED_IDENTITY_PROXY_SECRET_HEADER` | Optional header name override |
| `GOLEM_TRUSTED_IDENTITY_PROXY_IDENTITY_HEADER` | Optional header name override |

## Behaviour when enabled

Both headers are removed from every incoming request before it is logged, routed or bound to
agent parameters, so an agent can never read them (not through a header parameter either) and
they never reach an oplog. They are not echoed in responses or error messages.

| Route security | Secret header | Result |
| --- | --- | --- |
| Security scheme (OIDC) | absent | Unchanged: session cookie, otherwise `302` to the identity provider |
| Security scheme (OIDC) | valid, with a valid identity header | The agent is invoked with the asserted identity. The cookie is ignored, no session is stored, no `Set-Cookie` is sent |
| Security scheme (OIDC) | valid, identity header missing or invalid | `401` |
| Security scheme (OIDC) | not valid | `401`, never a redirect and never a fallback to the cookie |
| Test session header (`testSessionHeaderName`) | valid | Unchanged: the route's session header is honoured |
| Test session header (`testSessionHeaderName`) | absent or not valid | `401` |
| None | any | Unchanged (the headers are still removed) |

A `401` has a JSON body `{"code":"AUTH_UNAUTHORIZED","error":"..."}`. A malformed identity is a
`401` as well, not a `400`.

A secret header that occurs more than once is not valid; neither is an identity header that
occurs more than once. Secrets are compared in constant time (SHA-256 digests compared with
`subtle`), independent of the length of the presented value.

On test session header routes the identity still comes from the route's own header with its
existing semantics; the secret only gates it. This closes the hole that anyone who can reach
the port can forge an identity on such routes.

### Identity header

The identity header carries one JSON object, the same shape as the test session header:

```json
{
  "subject": "api-key-7f3a",
  "issuer": "https://keys.example.com",
  "email": "robot@example.com",
  "email_verified": true,
  "name": "Robot"
}
```

- `subject` (non-blank) and `issuer` (a URL) are required. There are no defaults on this path.
- Optional: `email`, `email_verified`, `name`, `given_name`, `family_name`, `picture`,
  `preferred_username`, `scopes`, `issued_at`, `expires_at` (RFC 3339).
- An `expires_at` in the past is rejected. Without it the assertion is good for this request;
  nothing is remembered between requests either way.

The agent receives an ordinary OIDC principal (`sub`, `issuer`, `email`, ... and `claims`
synthesised from subject and issuer). It cannot tell an asserted identity from an interactive
login, so use an `issuer` of your own to keep the two apart, and remember that `(issuer,
subject)` pairs asserted by the proxy can impersonate any interactive user if the proxy lets
clients choose them.

The identity provider of the security scheme is not contacted for an asserted identity.

## Threat model

- Whoever knows the secret can act as any identity on every OIDC and test session header route
  of every deployment served by this worker service. Treat it like a signing key.
- The proxy must be the only party that knows the secret, and the only network path over which
  the secret can reach the gateway.
- The proxy must remove client supplied copies of both headers from every request before adding
  its own. If it forwards a client supplied secret header unchanged, the request is answered
  with `401`; if it forwards a client supplied identity header next to its own secret, the
  client chooses its identity.
- Requests that do not go through the proxy (browsers using the OIDC login) are unaffected and
  cannot assert an identity without the secret. Isolate the gateway port on the network level
  anyway (for example a Kubernetes NetworkPolicy that only admits the proxy and the ingress):
  the secret is the only thing standing between a caller that reaches the port and every
  identity.
- The secret travels in a request header. Use TLS or a trusted network segment between the
  proxy and the gateway.
- The gateway does not rate limit failed attempts. A 32 byte random secret cannot be guessed,
  but put rate limiting in front of the gateway if the port is reachable by untrusted callers.

## Rotation

Without downtime:

1. Restart the worker service with `secret` = new secret and `previous_secret` = old secret.
2. Switch the proxy to the new secret.
3. Restart the worker service without `previous_secret`.

Or restart the worker service and the proxy together with a new `secret`; requests carrying
the old secret are answered with `401` in between.
