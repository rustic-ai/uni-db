//! Integration test verifying `BuiltinPlugin` registers its built-ins
//! successfully through the framework.

use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_builtin::BuiltinPlugin;

#[test]
fn builtin_plugin_registers_into_registry() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();

    let mut registrar = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut registrar).expect("register");
    registrar.commit_to_registry().expect("commit");

    // Smoke check: the placeholder built-in is present.
    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());
}

#[test]
fn builtin_locy_aggregates_resolve_by_name() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).unwrap();
    r.commit_to_registry().unwrap();

    for name in [
        "MIN", "MAX", "SUM", "MSUM", "COUNT", "AVG", "COLLECT", "MNOR", "MPROD",
    ] {
        let q = QName::builtin(name);
        assert!(
            registry.locy_aggregate(&q).is_some(),
            "expected {name} to be registered as a Locy aggregate"
        );
    }

    // SUM and MSUM share runtime but differ in monotonicity contract.
    let sum_sl = registry
        .locy_aggregate(&QName::builtin("SUM"))
        .unwrap()
        .aggregate
        .semilattice();
    assert!(!sum_sl.monotone_join, "SUM must be non-monotone");

    let msum_sl = registry
        .locy_aggregate(&QName::builtin("MSUM"))
        .unwrap()
        .aggregate
        .semilattice();
    assert!(
        msum_sl.monotone_join,
        "MSUM must be monotone (caller asserts non-negative inputs)"
    );
    assert!(
        !msum_sl.has_top,
        "MSUM is unbounded — has_top must be false"
    );
}

#[test]
fn builtin_system_procedure_is_registered() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).unwrap();
    r.commit_to_registry().unwrap();

    // `uni.system.echo` is registered under the qname `builtin.system.echo`
    // (the framework's namespace prefix). Real procedures will use the
    // `uni.` prefix once M4 ports them through the framework's alias
    // resolution layer.
    let q = QName::new("builtin", "system.echo");
    assert!(registry.procedure(&q).is_some());
}

#[test]
fn builtin_register_is_idempotent_after_remove() {
    let registry = PluginRegistry::new();

    // First load.
    {
        let plugin = BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
        plugin.register(&mut r).unwrap();
        r.commit_to_registry().unwrap();
    }

    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());

    // Remove and re-load — should succeed without DuplicateRegistration.
    registry.remove_plugin(&uni_plugin::PluginId::new(BuiltinPlugin::ID));
    assert!(registry.scalar_fn(&QName::builtin("identity")).is_none());

    {
        let plugin = BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
        plugin.register(&mut r).unwrap();
        r.commit_to_registry().unwrap();
    }

    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());
}

/// Pin the number of `CALL uni.algo.*` procedures the built-in plugin exposes.
///
/// The documented count ("N graph algorithms" on the docs landing page) is
/// checked against this assertion by `scripts/ci/check_documented_counts.py`,
/// the same way the plugin-surface count is wired to
/// `assert_eq!(kinds.len(), 22)` in `uni_plugin::surfaces`. Adding or removing
/// an algorithm fails here until the prose is updated.
///
/// Counting `uni.algo.*` string occurrences in the source is NOT equivalent:
/// it misses that `uni.path.expand` is not in the `algo` namespace at all, and
/// it cannot see that `algo.pageRank` and `algo.pagerank` are two dispatch
/// paths to one algorithm (the static-registry procedure and the Pregel
/// provider respectively) rather than two algorithms.
///
/// `BuiltinPlugin::register` deliberately does NOT register these — algorithm
/// qnames live in the `uni` namespace, so the host registers them under the
/// `uni` plugin id (`crates/uni/src/api/plugins.rs:190`). This mirrors that
/// call, capability set included; asserting against `BuiltinPlugin` instead
/// yields a vacuous zero.
#[test]
fn builtin_algorithm_count_is_pinned() {
    use uni_plugin::PluginId;
    use uni_plugin::capability::{Capability, CapabilitySet};

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
        Capability::GraphCompute,
    ]);
    let mut r = PluginRegistrar::new(PluginId::new("uni"), &caps, &registry);
    uni_plugin_builtin::algorithms::register_into(&mut r).expect("register algorithms");
    r.commit_to_registry().expect("commit");

    let mut algos: Vec<String> = registry
        .iter_algorithms()
        .into_iter()
        .map(|(q, _)| q)
        .filter(|q| q.namespace() == "uni" && q.local().starts_with("algo."))
        .map(|q| q.local().to_string())
        .collect();
    algos.sort();

    assert_eq!(
        algos.len(),
        43,
        "registered uni.algo.* procedures: {algos:?}"
    );

    // Aliases: qnames that dispatch to an algorithm another qname already
    // covers. Only PageRank has one today — `algo.pageRank` is the M4 static
    // adapter, `algo.pagerank` the M5c Pregel provider. The user-facing
    // "N graph algorithms" claim counts algorithms, not entry points, so the
    // documented number is the deduplicated one.
    const ALIAS_QNAMES: usize = 1;

    // Ground truth for `scripts/ci/check_documented_counts.py`. Keep the name
    // and the `= N;` shape — the script reads this literal.
    const DISTINCT_ALGORITHMS: usize = 42;

    assert_eq!(
        algos.len() - ALIAS_QNAMES,
        DISTINCT_ALGORITHMS,
        "documented graph-algorithm count is stale"
    );
}
