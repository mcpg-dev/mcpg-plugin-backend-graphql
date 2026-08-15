//! GraphQL backend binding plugin for mcpg (`kind: "graphql"`).
//!
//! Dispatches tool calls as GraphQL-over-HTTP POSTs: the call arguments
//! become the GraphQL `variables`, the operator-configured `operation`
//! is the `query`, and the `{ query, variables }` body is POSTed to the
//! endpoint. The transport / security / lifecycle machinery — per-cred
//! `reqwest::Client` cache, DNS-rebinding guard, per-call CEL +
//! `cred://` resolution, body-limit truncation, downstream-error retry
//! shaping — all come from the shared `net-core` crate via
//! [`NetworkProfileRuntime`]. This crate only adds the GraphQL framing:
//! the `{ query, variables }` body and the `errors[]`-aware envelope.
//!
//! Mirrors the http/grpc plugins' `register_profile` → resolve → exec →
//! envelope flow; the gateway recovers `is_error` from the
//! `downstreamError` slot — set on a non-200 status OR a non-empty
//! GraphQL `errors` array, matching the legacy inline path.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use mcpg_plugin_backend_net_core::exec;
use mcpg_plugin_backend_net_core::retry::{self, DownstreamHttpError};
use mcpg_plugin_backend_net_core::runtime::{NetworkProfileRuntime, build_expr_context};
use mcpg_plugin_backend_net_core::types::{
    HttpBackendMethod, HttpCallMode, HttpRequestProfile, HttpResponseSummary, RetrySafetyContext,
};
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    firstparty_manifest,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;

pub mod cdylib;

/// Embedded plugin descriptor — passed to the gateway registrar at
/// startup.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

/// Default per-call timeout. Matches the gateway binding default
/// (`default_binding_timeout_ms`) so a binding that omits `timeout_ms`
/// resolves to the identical value on either path.
fn default_timeout_ms() -> u64 {
    2_000
}
fn default_max_response_bytes() -> usize {
    65_536
}

/// Operator-facing spec the gateway serializes when calling
/// `register_profile`. Mirrors `GraphqlBackendConfig` in the gateway crate.
#[derive(Debug, Clone, Deserialize)]
struct GraphqlBackendSpec {
    url: String,
    operation: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: usize,
    #[serde(default)]
    allow_private_backends: bool,
}

/// Per-binding runtime state: the shared net-core resolution runtime
/// (URL + headers + client cache) plus the structural GraphQL
/// `operation` (the query document — fixed at config time, never
/// templated; the call args supply the `variables`).
#[derive(Clone)]
struct GraphqlProfile {
    net: NetworkProfileRuntime,
    operation: String,
}

/// `BackendPlugin` implementation for `kind: "graphql"`.
pub struct GraphqlBackendPlugin {
    manifest: PluginManifest,
    profiles: RwLock<BTreeMap<String, GraphqlProfile>>,
}

impl Default for GraphqlBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphqlBackendPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.graphql",
                name: "GraphQL Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
        }
    }
}

impl std::fmt::Debug for GraphqlBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphqlBackendPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

#[async_trait]
impl BackendPlugin for GraphqlBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "graphql"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &serde_json::Value,
        host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: GraphqlBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("GraphQL binding spec: {e}"),
            })?;

        if parsed.url.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "url must not be empty".into(),
            });
        }
        if !parsed.url.starts_with("http://") && !parsed.url.starts_with("https://") {
            return Err(BackendError::InvalidSpec {
                message: format!(
                    "url must start with http:// or https://, got '{}'",
                    parsed.url
                ),
            });
        }
        if parsed.operation.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "operation must not be empty".into(),
            });
        }
        if parsed.timeout_ms == 0 {
            return Err(BackendError::InvalidSpec {
                message: "timeout_ms must be greater than 0".into(),
            });
        }
        if parsed.max_response_bytes == 0 {
            return Err(BackendError::InvalidSpec {
                message: "max_response_bytes must be greater than 0".into(),
            });
        }

        // `url` is a transport-only routing fact the plugin treats as
        // plaintext (the request target), never as a credential-bearing
        // value — a `cred://` ref there is an operator mistake that would
        // leak a resolved secret into the URL. The gateway also enforces
        // this generically via the manifest `transport_only_fields`
        // declaration; this is the owning plugin's matching reject.
        if parsed.url.contains("cred://") {
            return Err(BackendError::InvalidSpec {
                message: "url must not contain a cred:// reference".into(),
            });
        }

        // GraphQL always POSTs a JSON body and expects a 200 JSON reply.
        let profile = HttpRequestProfile {
            url: parsed.url.clone(),
            method: HttpBackendMethod::Post,
            headers: parsed.headers.clone(),
            expected_status_codes: vec![200],
            require_json_response: true,
            max_response_bytes: parsed.max_response_bytes,
            timeout: std::time::Duration::from_millis(parsed.timeout_ms),
            allow_private_backends: parsed.allow_private_backends,
        };

        let secret_refs: Vec<String> = spec
            .get("__mcpg_secret_refs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();

        let net = NetworkProfileRuntime::register(
            backend_name,
            parsed.url,
            parsed.headers,
            profile,
            host,
            secret_refs,
        )
        .map_err(|e| BackendError::InvalidSpec {
            message: format!("GraphQL binding spec: {e}"),
        })?;

        self.profiles.write().await.insert(
            backend_name.to_owned(),
            GraphqlProfile {
                net,
                operation: parsed.operation,
            },
        );
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let profile = {
            let guard = self.profiles.read().await;
            guard
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };

        let variables: Value = if request.payload.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&request.payload).map_err(|e| BackendError::InvalidSpec {
                message: format!("GraphQL plugin payload is not valid JSON: {e}"),
            })?
        };

        let tool_name = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcpg-tool-name"))
            .map(|(_, v)| v.as_str())
            .unwrap_or(backend_name)
            .to_owned();

        let trace_headers: Vec<(String, String)> = request
            .headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                lower == "traceparent" || lower == "tracestate"
            })
            .cloned()
            .collect();
        let idempotency_key = request.idempotency.as_ref().map(|hint| hint.key.clone());
        let operator_has_idempotency_key = profile.net.operator_has_header("idempotency-key");

        // Standard GraphQL request body: { query, variables }.
        let graphql_body = serde_json::json!({
            "query": profile.operation,
            "variables": variables,
        });

        let expr_ctx = build_expr_context(&variables, &tool_name, &request);
        let envelope = match profile
            .net
            .resolve_client(&expr_ctx, &request, backend_name)
            .await
        {
            Ok(resolved) => {
                let response = exec::execute_http_call(
                    &resolved.client,
                    profile.net.profile(),
                    HttpCallMode::JsonBody,
                    &graphql_body,
                    None,
                    &trace_headers,
                    idempotency_key.as_deref(),
                    operator_has_idempotency_key,
                    &resolved.resolved_url,
                )
                .await;
                build_graphql_envelope(
                    &tool_name,
                    backend_name,
                    &resolved.resolved_url,
                    &profile.operation,
                    response.as_ref().map_err(String::as_str),
                )
            }
            Err(e) => build_graphql_envelope(
                &tool_name,
                backend_name,
                profile.net.profile().url.as_str(),
                &profile.operation,
                Err(&e),
            ),
        };

        Ok(envelope_response(envelope))
    }

    fn audit_metadata(&self, _backend_name: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();
        map.insert("graphql.transport".to_owned(), serde_json::json!("plugin"));
        map
    }
}

/// Serialize an envelope into a `BackendResponse`. The gateway reads
/// `downstreamError != null` to set `is_error`.
fn envelope_response(envelope: Value) -> BackendResponse {
    let payload = serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec());
    BackendResponse {
        payload,
        truncated: false,
    }
}

/// Whether a parsed GraphQL response carries a non-empty `errors` array.
fn has_graphql_errors(response_json: Option<&Value>) -> bool {
    response_json
        .and_then(|v| v.get("errors"))
        .and_then(|e| e.as_array())
        .is_some_and(|arr| !arr.is_empty())
}

/// The `downstreamError` for a GraphQL response that returned HTTP 200
/// but carried a non-empty `errors` array — an application-level error
/// the caller should inspect rather than blindly retry.
fn graphql_errors_downstream_error(errors: &Value) -> DownstreamHttpError {
    DownstreamHttpError {
        kind: "graphql_errors".to_owned(),
        code: "mcpg.downstream_graphql.errors".to_owned(),
        message: "GraphQL response returned a non-empty `errors` array.".to_owned(),
        retryable: false,
        retry_class: "do_not_retry".to_owned(),
        retry_after_ms: None,
        idempotency_hint: "potentially_non_idempotent".to_owned(),
        caller_retry_decision: "do_not_retry".to_owned(),
        retry_safety: "do_not_retry".to_owned(),
        backoff_strategy: "no_retry".to_owned(),
        minimum_backoff_ms: None,
        suggested_action: "inspect_graphql_errors".to_owned(),
        status_code: Some(200),
        details: serde_json::json!({ "errors": errors }),
    }
}

/// Build the GraphQL structured-content envelope. Carries the
/// GraphQL-specific fields the inline gateway path surfaced (url /
/// operation / statusCode / hasGraphqlErrors / response) plus the shared
/// `downstreamError` slot the gateway reads for `is_error` (non-200 OR
/// non-empty `errors` → error).
fn build_graphql_envelope(
    tool_name: &str,
    backend_name: &str,
    url: &str,
    operation: &str,
    response: Result<&HttpResponseSummary, &str>,
) -> Value {
    match response {
        Ok(summary) => {
            let response_json: Option<Value> = serde_json::from_str(&summary.body).ok();
            let graphql_errors = has_graphql_errors(response_json.as_ref());
            // Non-200 → status downstream error; else a 200 carrying a
            // non-empty `errors` array → graphql-errors downstream error.
            let downstream: Option<DownstreamHttpError> = retry::validate_expected_status_codes(
                &[200],
                summary.status_code,
                summary.retry_after_ms,
                RetrySafetyContext::PotentiallyNonIdempotentJsonCall,
            )
            .or_else(|| {
                if graphql_errors {
                    let errors = response_json
                        .as_ref()
                        .and_then(|v| v.get("errors"))
                        .cloned()
                        .unwrap_or(Value::Null);
                    Some(graphql_errors_downstream_error(&errors))
                } else {
                    None
                }
            });
            serde_json::json!({
                "toolName": tool_name,
                "profile": backend_name,
                "url": url,
                "operation": operation,
                "statusCode": summary.status_code,
                "durationMs": summary.duration_ms,
                "bodyTruncated": summary.body_truncated,
                "hasGraphqlErrors": graphql_errors,
                "body": summary.body,
                "response": response_json,
                "downstreamError": downstream
                    .map(|e| serde_json::to_value(e).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null),
            })
        }
        Err(error) => {
            let downstream = retry::transport_downstream_error(
                error,
                RetrySafetyContext::PotentiallyNonIdempotentJsonCall,
            );
            serde_json::json!({
                "toolName": tool_name,
                "profile": backend_name,
                "url": url,
                "operation": operation,
                "error": error,
                "downstreamError": serde_json::to_value(&downstream).unwrap_or(Value::Null),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_plugin_kind_is_graphql() {
        let plugin = GraphqlBackendPlugin::new();
        assert_eq!(plugin.kind(), "graphql");
    }

    #[test]
    fn manifest_advertises_first_party_id() {
        let plugin = GraphqlBackendPlugin::new();
        assert_eq!(plugin.manifest().id, "dev.mcpg.backend.graphql");
    }

    fn summary(status: u16, body: &str) -> HttpResponseSummary {
        HttpResponseSummary {
            status_code: status,
            content_type: Some("application/json".to_owned()),
            retry_after_ms: None,
            body: body.to_owned(),
            body_truncated: false,
            duration_ms: 1,
        }
    }

    #[test]
    fn envelope_200_no_errors_is_clean() {
        let env = build_graphql_envelope(
            "t",
            "b",
            "https://api.test/graphql",
            "query { me { id } }",
            Ok(&summary(200, r#"{"data":{"me":{"id":"1"}}}"#)),
        );
        assert_eq!(env["hasGraphqlErrors"], false);
        assert!(env["downstreamError"].is_null());
        assert_eq!(env["response"]["data"]["me"]["id"], "1");
    }

    #[test]
    fn envelope_200_with_errors_flags_downstream_error() {
        let env = build_graphql_envelope(
            "t",
            "b",
            "https://api.test/graphql",
            "query { me { id } }",
            Ok(&summary(200, r#"{"errors":[{"message":"boom"}]}"#)),
        );
        assert_eq!(env["hasGraphqlErrors"], true);
        assert!(!env["downstreamError"].is_null());
        assert_eq!(env["downstreamError"]["kind"], "graphql_errors");
    }

    #[test]
    fn envelope_non_200_flags_downstream_error() {
        let env = build_graphql_envelope(
            "t",
            "b",
            "https://api.test/graphql",
            "query { me { id } }",
            Ok(&summary(500, "{}")),
        );
        assert!(!env["downstreamError"].is_null());
    }

    #[tokio::test]
    async fn register_rejects_empty_operation() {
        let plugin = GraphqlBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "https://api.test/graphql",
            "operation": "",
        });
        let err = plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("should reject empty operation");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    // --- Conformance: the plugin is the single source of truth for its
    // defaults + value-validation + transport-only cred:// reject (the
    // checks that used to live in the gateway's GraphqlBackendConfig). ---

    /// Omitting `timeout_ms` / `max_response_bytes` resolves to the same
    /// defaults the gateway binding applied (2000ms / 64 KiB) — the
    /// secure/default value is materialized by the plugin, not the gateway.
    #[tokio::test]
    async fn register_applies_default_timeout_and_size() {
        let plugin = GraphqlBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "https://api.test/graphql",
            "operation": "query { me { id } }",
        });
        plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("registers with defaults");
        let guard = plugin.profiles.read().await;
        let profile = guard.get("test").expect("profile stored");
        assert_eq!(
            profile.net.profile().timeout,
            std::time::Duration::from_millis(2_000),
            "timeout_ms defaults to 2000 (gateway binding default)",
        );
        assert_eq!(
            profile.net.profile().max_response_bytes,
            65_536,
            "max_response_bytes defaults to 64 KiB",
        );
    }

    /// A bad scheme is rejected as `InvalidSpec` (value-validation moved
    /// from the gateway's `GraphqlBackendConfig::validate`).
    #[tokio::test]
    async fn register_rejects_non_http_url() {
        let plugin = GraphqlBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "ftp://api.test/graphql",
            "operation": "query { me { id } }",
        });
        let err = plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("should reject non-http url");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    /// `timeout_ms: 0` and `max_response_bytes: 0` are rejected.
    #[tokio::test]
    async fn register_rejects_zero_timeout_and_size() {
        let plugin = GraphqlBackendPlugin::new();
        for bad in [
            serde_json::json!({ "url": "https://g", "operation": "query { a }", "timeout_ms": 0 }),
            serde_json::json!({ "url": "https://g", "operation": "query { a }", "max_response_bytes": 0 }),
        ] {
            let err = plugin
                .register_profile("test", &bad, mcpg_plugin_protocol::noop_backend_host())
                .await
                .expect_err("should reject zero value");
            assert!(matches!(err, BackendError::InvalidSpec { .. }));
        }
    }

    /// A bare `cred://` ref in the transport-only `url` field is rejected —
    /// it is a plaintext routing fact, never a credential carrier, so a
    /// `cred://` there would leak a resolved secret into the URL.
    #[tokio::test]
    async fn register_rejects_cred_in_transport_only_field() {
        let plugin = GraphqlBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "cred://vault/url",
            "operation": "query { me { id } }",
        });
        let err = plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("should reject cred:// in transport-only field");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }
}
