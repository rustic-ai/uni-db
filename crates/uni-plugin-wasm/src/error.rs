//! Error types for the WASM loader.

use thiserror::Error;

/// Errors specific to the WASM loader.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WasmError {
    /// The supplied WASM bytes failed to parse or validate.
    #[error("wasm parse / validation failure: {0}")]
    InvalidWasm(String),

    /// Component instantiation failed.
    #[error("wasm instantiation failed: {0}")]
    Instantiate(String),

    /// A plugin export call trapped or returned a fn-error.
    #[error("wasm invoke failed: {0}")]
    Invoke(String),

    /// Arrow IPC marshalling across the linear-memory boundary failed.
    #[error("arrow IPC at wasm boundary: {0}")]
    Ipc(#[from] uni_plugin_wasm_rt::IpcError),

    /// Wall-clock or fuel deadline exceeded.
    #[error("wasm plugin exceeded resource limit: {0}")]
    ResourceLimit(String),

    /// The host registrar refused one of the plugin's registrations: a
    /// surface the registrar grants do not cover
    /// (`PluginError::CapabilityRequired`), or a qname already taken.
    /// Carried as the registrar's own error so the host reports it with
    /// the same kind a native or Rhai load does.
    #[error("plugin registration refused: {0}")]
    Registration(#[from] uni_plugin::PluginError),

    /// Internal / unexpected error.
    #[error("uni-plugin-wasm internal error: {0}")]
    Internal(String),

    /// The plugin's declared ABI range does not intersect any
    /// host-supported major (per the multi-version `Linker` cache in
    /// [`crate::multi_version`]).
    #[error("plugin abi {requested} unsupported; host majors: {supported:?}")]
    AbiUnsupported {
        /// The plugin's manifest `abi` range string.
        requested: String,
        /// Host-supported major versions.
        supported: Vec<u64>,
    },
}

impl uni_plugin_wasm_rt::pool::PoolResourceLimit for WasmError {
    fn resource_limit(msg: String) -> Self {
        Self::ResourceLimit(msg)
    }
}
