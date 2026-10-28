use std::sync::Arc;

use crate::fixtures::load_graph;
use crate::steps::assertions::parse_gherkin_value;
use crate::LocyWorld;
use cucumber::given;
use uni_locy::{FeatureValue, MockClassifier, NeuralClassifier};

#[given(regex = r"^(?:an empty graph|any graph)$")]
async fn empty_or_any_graph(world: &mut LocyWorld) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
}

#[given(regex = r"^the (.+) graph$")]
async fn named_graph(world: &mut LocyWorld, graph_name: String) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
    load_graph(world.db(), &graph_name)
        .await
        .unwrap_or_else(|e| panic!("Failed to load graph '{}': {}", graph_name, e));
}

#[given("having executed:")]
async fn having_executed(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    // A `having executed:` step without an attached docstring is a malformed
    // scenario: the intended graph setup would be silently skipped, letting the
    // scenario pass against an empty graph. Fail loudly instead, mirroring the
    // sibling `when evaluating ...` step (when_evaluate.rs).
    let query = step
        .docstring()
        .expect("Expected a docstring with the setup query for `having executed:`");
    let session = world.db().session();
    let tx = session
        .tx()
        .await
        .unwrap_or_else(|e| panic!("Failed to start transaction: {}", e));
    tx.execute(query)
        .await
        .unwrap_or_else(|e| panic!("Setup query failed: {}", e));
    tx.commit()
        .await
        .unwrap_or_else(|e| panic!("Failed to commit setup query: {}", e));
}

/// Register a node label imperatively in the schema manager. Required
/// for tests that exercise `uni.algo.*` procedures (degree_centrality,
/// pagerank, etc.) which validate labels against the registered schema
/// before running. The TCK harness defaults to schema-less; this step
/// declares the label so the algorithm sees the corresponding nodes.
#[given(regex = r#"^a registered node label ['"](.+)['"]$"#)]
async fn register_node_label(world: &mut LocyWorld, name: String) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
    let schema = world.db().schema_manager();
    match schema.add_label(&name) {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => panic!("Failed to register node label '{}': {}", name, e),
    }
}

/// Register an edge type imperatively in the schema manager. The
/// `from`/`to` label lists must already be registered (use the
/// `a registered node label ...` step first). Required for
/// neighbor-aggregator FEATURE expressions and topology procedures
/// that scope by edge type.
#[given(regex = r#"^a registered edge type ['"](.+)['"] from ['"](.+)['"] to ['"](.+)['"]$"#)]
async fn register_edge_type(
    world: &mut LocyWorld,
    name: String,
    from_label: String,
    to_label: String,
) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
    let schema = world.db().schema_manager();
    match schema.add_edge_type(&name, vec![from_label], vec![to_label]) {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => panic!("Failed to register edge type '{}': {}", name, e),
    }
}

#[given(regex = r#"^the parameter (\w+) = (.+)$"#)]
fn set_parameter(world: &mut LocyWorld, name: String, value_str: String) {
    world.add_param(name, parse_gherkin_value(&value_str));
}

// ───────────────────────────────────────────────────────────────────────
// Phase B Slice 3: neural classifier registration for tests
// ───────────────────────────────────────────────────────────────────────

/// Register a constant-output mock classifier under a model name. The
/// model declaration in the Locy program supplies INPUT/OUTPUT/USING —
/// this step provides the actual runtime classifier instance.
#[given(regex = r#"^a registered mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+)$"#)]
fn register_mock_classifier_constant(world: &mut LocyWorld, name: String, value: f64) {
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::constant("mock/constant", value));
    world.classifier_registry.insert(name, classifier);
}

/// Register a feature-driven mock that pulls a single Float feature
/// named `feature_name` and clamps it to `[0, 1]`. Useful when scenarios
/// want per-row variation without standing up a full provider.
#[given(
    regex = r#"^a registered mock classifier ['"](.+)['"] driven by Float feature ['"](.+)['"]$"#
)]
fn register_mock_classifier_feature(world: &mut LocyWorld, name: String, feature_name: String) {
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::new("mock/feature", move |inp| {
            match inp.features.get(&feature_name) {
                Some(FeatureValue::Float(v)) => v.clamp(0.0, 1.0),
                Some(FeatureValue::Int(v)) => (*v as f64).clamp(0.0, 1.0),
                _ => 0.0,
            }
        }));
    world.classifier_registry.insert(name, classifier);
}

/// Register a mock that classifies a **string** feature: returns
/// `0.9` if `feature_name == "high"`, `0.1` otherwise. Lets TCK
/// scenarios verify that string-typed property-access feature
/// expressions (Phase B Slice 3 follow-up) feed the classifier.
#[given(
    regex = r#"^a registered mock classifier ['"](.+)['"] returning 0\.9 when string feature ['"](.+)['"] equals ['"](.+)['"]$"#
)]
fn register_mock_classifier_string_match(
    world: &mut LocyWorld,
    name: String,
    feature_name: String,
    target_value: String,
) {
    let classifier: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::new(
        "mock/string-match",
        move |inp| match inp.features.get(&feature_name) {
            Some(FeatureValue::String(s)) if *s == target_value => 0.9,
            _ => 0.1,
        },
    ));
    world.classifier_registry.insert(name, classifier);
}

/// Phase C B1-B3 follow-up: register a `CalibratedClassifier`
/// that wraps a constant mock with a conformal-style identity
/// calibrator (returns the input unchanged but exposes a
/// confidence band of `[p-q, p+q]` clipped to [0,1] with a
/// pinned quantile). Used to assert that EXPLAIN surfaces both
/// `calibrated_probability` and `confidence_band`.
#[given(
    regex = r#"^a registered Calibrated mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+) with conformal quantile ([0-9]*\.?[0-9]+)$"#
)]
fn register_calibrated_mock_classifier(
    world: &mut LocyWorld,
    name: String,
    value: f64,
    quantile: f64,
) {
    use uni_locy::calibration::ConformalPredictor;
    use uni_locy::CalibratedClassifier;
    let base: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::constant(
        format!("mock/base-{}", name),
        value,
    ));
    let calibrator: Arc<dyn uni_locy::calibration::Calibrator> = Arc::new(ConformalPredictor {
        alpha: 0.1,
        quantile,
    });
    let wrapped = CalibratedClassifier::new(name.clone(), base, calibrator);
    let arc: Arc<dyn NeuralClassifier> = Arc::new(wrapped);
    world.classifier_registry.insert(name, arc);
}

/// Register the historical Candle API's CPU linear classifier,
/// `CandleLinearClassifier` with a deterministic single-feature
/// logistic-regression fixture. Weights are chosen so that an input
/// of 1.0 produces sigmoid ≈ 0.9 and 0.0 produces sigmoid ≈ 0.1.
/// The fixture is written to a temp file per-scenario, so no
/// checked-in binary blobs are required.
///
/// The `binding_name` matches the model's `INPUT (<name>)` clause —
/// that's the key under which the runtime populates `ClassifyInput`.
/// For `FEATURES s.score`, the binding name is `s` and the
/// classifier receives `features["s"] = Float(<row's score>)`.
#[given(regex = r#"^a Candle classifier ['"](.+)['"] over Float input binding ['"](.+)['"]$"#)]
fn register_candle_classifier_float(world: &mut LocyWorld, name: String, binding_name: String) {
    use safetensors::tensor::{serialize_to_file, Dtype, TensorView};
    use uni_locy::CandleLinearClassifier;

    // Solve logit(0.9) ≈ 2.197, logit(0.1) ≈ -2.197 for f(1.0)/f(0.0):
    // weight = 4.394, bias = -2.197.
    const WEIGHT: f32 = 4.394;
    const BIAS: f32 = -2.197;

    let weight_bytes = WEIGHT.to_le_bytes();
    let bias_bytes = BIAS.to_le_bytes();
    let tensors = [
        (
            "weight",
            TensorView::new(Dtype::F32, vec![1], &weight_bytes).unwrap(),
        ),
        (
            "bias",
            TensorView::new(Dtype::F32, vec![1], &bias_bytes).unwrap(),
        ),
    ];

    let temp = tempfile::Builder::new()
        .prefix("candle-fixture-")
        .suffix(".safetensors")
        .tempfile()
        .expect("create temp fixture");
    serialize_to_file(tensors, None, temp.path()).expect("save fixture safetensors");

    let classifier =
        CandleLinearClassifier::load(format!("candle/{}", name), vec![binding_name], temp.path())
            .expect("load Candle fixture");
    let arc: Arc<dyn NeuralClassifier> = Arc::new(classifier);
    world.classifier_registry.insert(name, arc);
}

/// Register a mock that increments a shared counter every time
/// `classify` is invoked, then returns a constant value. Used to
/// observe memoization / cross-clause batching behavior.
#[given(regex = r#"^a counting mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+)$"#)]
fn register_counting_mock(world: &mut LocyWorld, name: String, value: f64) {
    let counter: Arc<std::sync::atomic::AtomicUsize> =
        Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_ref = Arc::clone(&counter);
    let value = value.clamp(0.0, 1.0);
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::new("mock/counting", move |_| {
            counter_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            value
        }));
    world.classifier_registry.insert(name.clone(), classifier);
    world.classifier_call_counts.insert(name, counter);
}
