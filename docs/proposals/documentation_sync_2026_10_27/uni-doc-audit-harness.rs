mod locy_adverse_drug_reaction {
pub async fn run() -> uni_db::Result<()> {
use std::sync::Arc;
use uni_db::{DataType, Uni, Result};
use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};

let db = Uni::in_memory().build().await?;
db.schema()
    .label("Report")
        .property("report_id", DataType::String)
        .property("report_count", DataType::Float64)
        .property("is_signal", DataType::Bool)
    .done()
    .apply()
    .await?;


let session = db.session();
let tx = session.tx().await?;
let rows: &[(&str, f64, bool)] = &[
    ("R01", 9.2, true),  ("R02", 2.0, false),
    ("R03", 8.7, true),  ("R04", 1.6, false),
    ("R05", 2.4, false), ("R06", 3.0, false),
    ("R07", 9.5, true),  ("R08", 2.2, false),
    ("R09", 8.9, true),  ("R10", 2.6, false),
    ("R11", 3.1, false), ("R12", 2.0, false),
];
for (rid, rc, is_signal) in rows {
    let q = format!(
        "CREATE (:Report {{report_id: '{}', report_count: {}, is_signal: {}}})",
        rid, rc, is_signal
    );
    tx.execute(&q).await?;
}
tx.commit().await?;


let classifier: Arc<dyn NeuralClassifier> = Arc::new(
    MockClassifier::new("signal_score", |inp| {
        let rc = match inp.features.get("r") {
            Some(FeatureValue::Float(v)) => *v,
            _ => 0.0,
        };
        let z = (rc - 5.0) * 0.9 - 0.5;
        let p = 1.0 / (1.0 + (-z).exp());
        let p_sharp = 1.0 / (1.0 + (-3.5 * (p - 0.5)).exp());
        p_sharp.clamp(0.0, 1.0)
    })
);
let mut config = LocyConfig::default();
config.classifier_registry.insert("signal_score".to_string(), classifier);


let program = r#"
CREATE MODEL signal_score AS
  INPUT (r)
  FEATURES r.report_count
  OUTPUT PROB credibility
  USING xervo('classify/adr-signal-v1')

CREATE RULE scored_reports AS
  MATCH (r:Report)
  YIELD KEY r, signal_score(r.report_count) AS credibility
"#;
let result = session.locy_with(program).with_config(config.clone()).run().await?;
let n = result.derived_rules().await?.get("scored_reports").map(|v| v.len()).unwrap_or(0);
println!("Scored {} reports", n);


let calibrate_program = r#"
CREATE MODEL signal_score AS
  INPUT (r)
  FEATURES r.report_count
  OUTPUT PROB credibility
  USING xervo('classify/adr-signal-v1')

CALIBRATE signal_score
  ON MATCH (r:Report)
  TARGET r.is_signal
  METHOD platt_scaling
"#;
let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;
println!("command_results: {} entries", calib.command_results().len());


let explain_program = format!("{}{}", program, r#"

EXPLAIN RULE scored_reports WHERE r.report_id = 'R01'
"#);
let explain = session.locy_with(&explain_program).with_config(config).run().await?;
println!("EXPLAIN command_results: {}", explain.command_results().len());

Ok(())
}
}
mod locy_compliance_remediation {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Internet")
        .property("name", DataType::String)
    .label("Service")
        .property("name", DataType::String)
        .property("cve_score", DataType::Float64)
    .edge_type("EXPOSES", &["Internet"], &["Service"])
    .edge_type("DEPENDS_ON", &["Service"], &["Service"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Internet {name: 'public'})").await?;
tx.execute("CREATE (:Service {name: 'api', cve_score: 9.1})").await?;
tx.execute("CREATE (:Service {name: 'worker', cve_score: 4.0})").await?;
tx.execute("CREATE (:Service {name: 'db', cve_score: 8.4})").await?;
tx.execute("MATCH (i:Internet {name:'public'}), (s:Service {name:'api'}) CREATE (i)-[:EXPOSES]->(s)").await?;
tx.execute("MATCH (a:Service {name:'api'}), (w:Service {name:'worker'}) CREATE (a)-[:DEPENDS_ON]->(w)").await?;
tx.execute("MATCH (w:Service {name:'worker'}), (d:Service {name:'db'}) CREATE (w)-[:DEPENDS_ON]->(d)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE depends_on AS
MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
YIELD KEY a, KEY b

CREATE RULE depends_on AS
MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
WHERE mid IS depends_on TO b
YIELD KEY a, KEY b

CREATE RULE exposed AS
MATCH (i:Internet)-[:EXPOSES]->(s:Service)
YIELD KEY s

CREATE RULE exposed AS
MATCH (i:Internet)-[:EXPOSES]->(entry:Service)
WHERE entry IS depends_on TO s
YIELD KEY s

CREATE RULE vulnerable AS
MATCH (s:Service)
WHERE s.cve_score >= 7
YIELD KEY s

CREATE RULE non_compliant AS
MATCH (s:Service)
WHERE s IS vulnerable, s IS exposed
YIELD KEY s, 'patch-now' AS action

QUERY non_compliant WHERE s.name = s.name RETURN s.name AS service, action"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
mod locy_drug_drug_interaction {
pub async fn run() -> uni_db::Result<()> {
use std::sync::Arc;
use uni_db::{DataType, Uni, Result};
use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};

let db = Uni::in_memory().build().await?;
db.schema()
    .label("InteractionRecord")
        .property("pair_id", DataType::String)
        .property("severity", DataType::Float64)
        .property("is_dangerous", DataType::Bool)
    .done()
    .apply()
    .await?;


let session = db.session();
let tx = session.tx().await?;
let rows: &[(&str, f64, bool)] = &[
    ("P01", 0.55, true),  ("P02", 0.18, false),
    ("P03", 0.60, true),  ("P04", 0.22, false),
    ("P05", 0.42, true),  ("P06", 0.25, false),
    ("P07", 0.65, true),  ("P08", 0.20, false),
    ("P09", 0.50, true),  ("P10", 0.30, false),
];
for (pid, sev, danger) in rows {
    let q = format!(
        "CREATE (:InteractionRecord {{pair_id: '{}', severity: {}, is_dangerous: {}}})",
        pid, sev, danger
    );
    tx.execute(&q).await?;
}
tx.commit().await?;


let classifier: Arc<dyn NeuralClassifier> = Arc::new(
    MockClassifier::new("interaction_score", |inp| {
        let sev = match inp.features.get("rec") {
            Some(FeatureValue::Float(v)) => *v,
            _ => 0.0,
        };
        let z = (sev - 0.40) * 8.0 - 0.3;
        let p = 1.0 / (1.0 + (-z).exp());
        let p_sharp = 1.0 / (1.0 + (-4.0 * (p - 0.5)).exp());
        p_sharp.clamp(0.0, 1.0)
    })
);
let mut config = LocyConfig::default();
config.classifier_registry.insert("interaction_score".to_string(), classifier);


let program = r#"
CREATE MODEL interaction_score AS
  INPUT (rec)
  FEATURES rec.severity
  OUTPUT PROB risk
  USING xervo('classify/ddi-v1')

CREATE RULE scored_interactions AS
  MATCH (rec:InteractionRecord)
  YIELD KEY rec, interaction_score(rec.severity) AS risk
"#;
let result = session.locy_with(program).with_config(config.clone()).run().await?;
let n = result.derived_rules().await?.get("scored_interactions").map(|v| v.len()).unwrap_or(0);
println!("Scored {} interaction pairs", n);


let calibrate_program = r#"
CREATE MODEL interaction_score AS
  INPUT (rec)
  FEATURES rec.severity
  OUTPUT PROB risk
  USING xervo('classify/ddi-v1')

CALIBRATE interaction_score
  ON MATCH (rec:InteractionRecord)
  TARGET rec.is_dangerous
  METHOD platt_scaling
"#;
let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;
println!("command_results: {} entries", calib.command_results().len());


let explain_program = format!("{}{}", program, r#"

EXPLAIN RULE scored_interactions WHERE rec.pair_id = 'P01'
"#);
let explain = session.locy_with(&explain_program).with_config(config).run().await?;
println!("EXPLAIN command_results: {}", explain.command_results().len());

Ok(())
}
}
mod locy_fraud_risk_propagation {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Account")
        .property("id", DataType::String)
        .property("flagged", DataType::Bool)
    .edge_type("TRANSFER", &["Account"], &["Account"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Account {id: 'A1', flagged: true})").await?;
tx.execute("CREATE (:Account {id: 'A2', flagged: false})").await?;
tx.execute("CREATE (:Account {id: 'A3', flagged: false})").await?;
tx.execute("CREATE (:Account {id: 'A4', flagged: false})").await?;
tx.execute("MATCH (a2:Account {id:'A2'}), (a1:Account {id:'A1'}) CREATE (a2)-[:TRANSFER]->(a1)").await?;
tx.execute("MATCH (a4:Account {id:'A4'}), (a2:Account {id:'A2'}) CREATE (a4)-[:TRANSFER]->(a2)").await?;
tx.execute("MATCH (a1:Account {id:'A1'}), (a3:Account {id:'A3'}) CREATE (a1)-[:TRANSFER]->(a3)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE risky_seed AS
MATCH (a:Account)
WHERE a.flagged = true
YIELD KEY a

CREATE RULE risky AS
MATCH (a:Account)
WHERE a IS risky_seed
YIELD KEY a

CREATE RULE risky AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE b IS risky
YIELD KEY a

CREATE RULE clean AS
MATCH (a:Account)
WHERE a IS NOT risky
YIELD KEY a

QUERY risky WHERE a.id = a.id RETURN a.id AS risky_account
QUERY clean WHERE a.id = a.id RETURN a.id AS clean_account"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
mod locy_infrastructure_blast_radius {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Service")
        .property("name", DataType::String)
    .edge_type("CALLS", &["Service"], &["Service"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Service {name: 'api'})").await?;
tx.execute("CREATE (:Service {name: 'gateway'})").await?;
tx.execute("CREATE (:Service {name: 'worker'})").await?;
tx.execute("CREATE (:Service {name: 'db'})").await?;
tx.execute("CREATE (:Service {name: 'cache'})").await?;
tx.execute("MATCH (a:Service {name:'api'}), (g:Service {name:'gateway'}) CREATE (a)-[:CALLS]->(g)").await?;
tx.execute("MATCH (g:Service {name:'gateway'}), (w:Service {name:'worker'}) CREATE (g)-[:CALLS]->(w)").await?;
tx.execute("MATCH (w:Service {name:'worker'}), (d:Service {name:'db'}) CREATE (w)-[:CALLS]->(d)").await?;
tx.execute("MATCH (w:Service {name:'worker'}), (c:Service {name:'cache'}) CREATE (w)-[:CALLS]->(c)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE impacts AS
MATCH (a:Service)-[:CALLS]->(b:Service)
YIELD KEY a, KEY b

CREATE RULE impacts AS
MATCH (a:Service)-[:CALLS]->(mid:Service)
WHERE mid IS impacts TO b
YIELD KEY a, KEY b

QUERY impacts WHERE a.name = 'api' RETURN b.name AS impacted_service"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
mod locy_predictive_maintenance {
pub async fn run() -> uni_db::Result<()> {
use std::sync::Arc;
use uni_db::{DataType, Uni, Result};
use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Equipment")
        .property("equipment_id", DataType::String)
        .property("air_temp", DataType::Float64)
        .property("actual_failed", DataType::Bool)
    .done()
    .apply()
    .await?;


let session = db.session();
let tx = session.tx().await?;
// 8 equipment instances, 3 actually failed (higher air_temp).
let rows: &[(&str, f64, bool)] = &[
    ("e01", 301.0, true), ("e02", 298.4, false),
    ("e03", 298.6, false), ("e04", 300.5, true),
    ("e05", 298.7, false), ("e06", 299.0, false),
    ("e07", 300.8, true), ("e08", 298.5, false),
];
for (eid, t, failed) in rows {
    let q = format!(
        "CREATE (:Equipment {{equipment_id: '{}', air_temp: {}, actual_failed: {}}})",
        eid, t, failed
    );
    tx.execute(&q).await?;
}
tx.commit().await?;


let classifier: Arc<dyn NeuralClassifier> = Arc::new(
    MockClassifier::new("failure_likelihood", |inp| {
        let air = match inp.features.get("e") {
            Some(FeatureValue::Float(v)) => *v,
            _ => 0.0,
        };
        let z = (air - 298.5) * 1.5 - 1.0;
        let p = 1.0 / (1.0 + (-z).exp());
        let p_sharp = 1.0 / (1.0 + (-3.0 * (p - 0.5)).exp());
        p_sharp.clamp(0.0, 1.0)
    })
);
let mut config = LocyConfig::default();
config.classifier_registry.insert("failure_likelihood".to_string(), classifier);


let program = r#"
CREATE MODEL failure_likelihood AS
  INPUT (e)
  FEATURES e.air_temp
  OUTPUT PROB will_fail
  USING xervo('classify/failure-likelihood-v1')

CREATE RULE asset_risk AS
  MATCH (e:Equipment)
  YIELD KEY e, failure_likelihood(e.air_temp) AS risk
"#;
let result = session.locy_with(program).with_config(config.clone()).run().await?;
let asset_rows = result.derived_rules().await?.get("asset_risk").map(|v| v.len()).unwrap_or(0);
println!("Scored {} equipment", asset_rows);


let calibrate_program = r#"
CREATE MODEL failure_likelihood AS
  INPUT (e)
  FEATURES e.air_temp
  OUTPUT PROB will_fail
  USING xervo('classify/failure-likelihood-v1')

CALIBRATE failure_likelihood
  ON MATCH (e:Equipment)
  TARGET e.actual_failed
  METHOD platt_scaling
"#;
let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;
println!("command_results: {} entries", calib.command_results().len());


let explain_program = format!("{}{}", program, r#"

EXPLAIN RULE asset_risk WHERE e.equipment_id = 'e01'
"#);
let explain = session.locy_with(&explain_program).with_config(config).run().await?;
println!("EXPLAIN command_results: {}", explain.command_results().len());

Ok(())
}
}
mod locy_probabilistic_risk_scoring {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Vendor")
        .property("name", DataType::String)
    .label("Component")
        .property("name", DataType::String)
    .label("QualitySignal")
        .property("name", DataType::String)
        .property("pass_rate", DataType::Float64)
    .edge_type("SUPPLIES", &["Vendor"], &["Component"])
    .edge_type("HAS_SIGNAL", &["Component"], &["QualitySignal"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Vendor {name: 'ReliaCorp'})").await?;
tx.execute("CREATE (:Vendor {name: 'QuickParts'})").await?;
tx.execute("CREATE (:Vendor {name: 'BudgetSupply'})").await?;
tx.execute("CREATE (:Component {name: 'Sensor'})").await?;
tx.execute("CREATE (:Component {name: 'Motor'})").await?;
tx.execute("CREATE (:Component {name: 'Controller'})").await?;
tx.execute("CREATE (:Component {name: 'Battery'})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Thermal Test', pass_rate: 0.95})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Vibration Test', pass_rate: 0.90})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Voltage Tolerance', pass_rate: 0.85})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Humidity Test', pass_rate: 0.92})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Load Test', pass_rate: 0.88})").await?;
tx.execute("CREATE (:QualitySignal {name: 'EMC Test', pass_rate: 0.75})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Cycle Life', pass_rate: 0.80})").await?;
tx.execute("CREATE (:QualitySignal {name: 'Drop Test', pass_rate: 0.70})").await?;
tx.execute("MATCH (v:Vendor {name:'ReliaCorp'}), (c:Component {name:'Sensor'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (v:Vendor {name:'ReliaCorp'}), (c:Component {name:'Motor'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (v:Vendor {name:'QuickParts'}), (c:Component {name:'Motor'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (v:Vendor {name:'QuickParts'}), (c:Component {name:'Controller'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (v:Vendor {name:'BudgetSupply'}), (c:Component {name:'Controller'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (v:Vendor {name:'BudgetSupply'}), (c:Component {name:'Battery'}) CREATE (v)-[:SUPPLIES]->(c)").await?;
tx.execute("MATCH (c:Component {name:'Sensor'}), (s:QualitySignal {name:'Thermal Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Sensor'}), (s:QualitySignal {name:'Vibration Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Motor'}), (s:QualitySignal {name:'Voltage Tolerance'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Motor'}), (s:QualitySignal {name:'Humidity Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Controller'}), (s:QualitySignal {name:'Load Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Controller'}), (s:QualitySignal {name:'EMC Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Battery'}), (s:QualitySignal {name:'Cycle Life'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.execute("MATCH (c:Component {name:'Battery'}), (s:QualitySignal {name:'Drop Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE component_failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk

CREATE RULE vendor_reliability AS
MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)
WHERE c IS component_failure_risk
FOLD reliability = MPROD(1.0 - risk)
YIELD KEY v, reliability

QUERY component_failure_risk RETURN c.name AS component, risk
QUERY vendor_reliability RETURN v.name AS vendor, reliability"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
mod locy_rbac_priority {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("User")
        .property("name", DataType::String)
    .label("Resource")
        .property("name", DataType::String)
    .edge_type("ALLOWED", &["User"], &["Resource"])
    .edge_type("DENIED", &["User"], &["Resource"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:User {name: 'alice'})").await?;
tx.execute("CREATE (:User {name: 'bob'})").await?;
tx.execute("CREATE (:Resource {name: 'prod-db'})").await?;
tx.execute("MATCH (u:User {name:'alice'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:ALLOWED]->(r)").await?;
tx.execute("MATCH (u:User {name:'bob'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:ALLOWED]->(r)").await?;
tx.execute("MATCH (u:User {name:'bob'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:DENIED]->(r)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE access PRIORITY 1 AS
MATCH (u:User)-[:ALLOWED]->(r:Resource)
YIELD KEY u, KEY r, 1 AS decision_code

CREATE RULE access PRIORITY 2 AS
MATCH (u:User)-[:DENIED]->(r:Resource)
YIELD KEY u, KEY r, 2 AS decision_code"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
mod locy_supply_chain_provenance {
pub async fn run() -> uni_db::Result<()> {
use uni_db::{DataType, Uni, Result};

let db = Uni::in_memory().build().await?;


db.schema()
    .label("Part")
        .property("sku", DataType::String)
        .property("kind", DataType::String)
    .edge_type("SOURCED_FROM", &["Part"], &["Part"])
    .apply()
    .await?;

println!("Schema created");


let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Part {sku: 'C1', kind: 'finished'})").await?;
tx.execute("CREATE (:Part {sku: 'B1', kind: 'subassembly'})").await?;
tx.execute("CREATE (:Part {sku: 'B2', kind: 'subassembly'})").await?;
tx.execute("CREATE (:Part {sku: 'R1', kind: 'raw'})").await?;
tx.execute("CREATE (:Part {sku: 'R2', kind: 'raw'})").await?;
tx.execute("MATCH (c:Part {sku:'C1'}), (b1:Part {sku:'B1'}) CREATE (c)-[:SOURCED_FROM]->(b1)").await?;
tx.execute("MATCH (c:Part {sku:'C1'}), (b2:Part {sku:'B2'}) CREATE (c)-[:SOURCED_FROM]->(b2)").await?;
tx.execute("MATCH (b1:Part {sku:'B1'}), (r1:Part {sku:'R1'}) CREATE (b1)-[:SOURCED_FROM]->(r1)").await?;
tx.execute("MATCH (b2:Part {sku:'B2'}), (r2:Part {sku:'R2'}) CREATE (b2)-[:SOURCED_FROM]->(r2)").await?;
tx.commit().await?;
println!("Seeded graph data");


let program = r#"CREATE RULE upstream AS
MATCH (a:Part)-[:SOURCED_FROM]->(b:Part)
YIELD KEY a, KEY b

CREATE RULE upstream AS
MATCH (a:Part)-[:SOURCED_FROM]->(mid:Part)
WHERE mid IS upstream TO b
YIELD KEY a, KEY b

QUERY upstream WHERE a.sku = 'C1' RETURN b.sku AS supplier_sku, b.kind AS supplier_kind"#;


let session = db.session();
let result = session.locy(program).await?;
println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());
println!("Iterations: {}", result.stats().total_iterations);
println!("Queries executed: {}", result.stats().queries_executed);
for (name, rows) in result.derived_rules().await? {
    println!("{}: {} row(s)", name, rows.len());
}

if let Some(rows) = result.rows() {
    println!("Rows: {:?}", rows);
}

Ok(())
}
}
#[tokio::main]
async fn main() -> uni_db::Result<()> {
locy_adverse_drug_reaction::run().await?;
locy_compliance_remediation::run().await?;
locy_drug_drug_interaction::run().await?;
locy_fraud_risk_propagation::run().await?;
locy_infrastructure_blast_radius::run().await?;
locy_predictive_maintenance::run().await?;
locy_probabilistic_risk_scoring::run().await?;
locy_rbac_priority::run().await?;
locy_supply_chain_provenance::run().await?;
Ok(())
}