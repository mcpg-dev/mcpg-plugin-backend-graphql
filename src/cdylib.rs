//! cdylib sync bridge — adapts the async [`GraphqlBackendPlugin`]
//! ([`mcpg_plugin_protocol::BackendPlugin`]) onto the sync FFI trait the
//! cdylib vtable expects ([`SyncBackendPlugin`]).
//!
//! Minimal, like the grpc bridge: GraphQL is request/reply, so it
//! inherits the buffered `execute_streaming` default and the no-op
//! `cancel_stream` / `complete_template_variable`. Only manifest / kind
//! / register_profile / execute / audit_metadata are forwarded, each
//! `block_on`-ing the async inner plugin on a private multi-thread
//! runtime. The make-time [`HostHandle`] is wrapped as an
//! `Arc<dyn BackendHost>` (via [`HostHandleBackendHost`]) for
//! `register_profile`'s `cred://` resolution.

use std::sync::Arc;

use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
};
use mcpg_plugin_sdk::ffi::SyncBackendPlugin;
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};

use crate::GraphqlBackendPlugin;

fn build_bridge_runtime(thread_name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name.to_owned())
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("graphql cdylib: tokio runtime init failed: {e}"))
}

/// `SyncBackendPlugin` bridge over [`GraphqlBackendPlugin`].
pub struct GraphqlBackendCdylib {
    inner: GraphqlBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl GraphqlBackendCdylib {
    /// Infallible cdylib factory. `config_json` is ignored — GraphQL
    /// carries no plugin-level config (per-binding url/operation/headers
    /// arrive via `register_profile`).
    pub fn from_host_config(_config_json: &str, host: HostHandle) -> Self {
        Self {
            inner: GraphqlBackendPlugin::new(),
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: build_bridge_runtime("mcpg-backend-graphql"),
        }
    }
}

impl SyncBackendPlugin for GraphqlBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(
        &self,
        profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, serde_json::Value> {
        BackendPlugin::audit_metadata(&self.inner, profile_name)
    }
}

// cdylib export — one `backend` entity under `dev.mcpg.backend.graphql`.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.graphql",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    // GraphQL: pipeline-capable (a `kind: graphql` pipeline step), no
    // dynamic tool list, label defaults to the kind. Health is an HTTP
    // reachability probe on `url` (GraphQL POSTs over HTTP/1.1, like
    // http/soap — unlike gRPC's TCP-only probe). `url` is a transport-only
    // routing fact — the gateway's generic spec-walk asserts no `cred://`
    // lands there (`operation` is the query document, not a routing fact).
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        health_probe: ::mcpg_plugin_protocol::manifest::HealthProbeDecl::Http {
            path: ::std::string::String::new(),
        },
        pipeline_capable: true,
        transport_only_fields: ::std::vec!["/url".to_owned()],
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: GraphqlBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                GraphqlBackendCdylib::from_host_config(cfg, host),
        },
    ],
}
