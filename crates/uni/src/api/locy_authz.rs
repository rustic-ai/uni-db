// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Authorization of Locy programs against the registered `AuthzPolicy` chain.
//!
//! Cypher authorizes each statement before it runs. Locy did not: no entry
//! point consulted a policy, so a principal a policy refused could still
//! read through a rule, write through a Cypher command or a `DERIVE`, and
//! run an administrative procedure (`tx.locy("CALL uni.admin.…")`).
//!
//! A program is authorized as a whole, before any of it runs, so a refusal
//! leaves nothing half done. It makes one request per verb it needs, with the
//! program text as the resource path (Cypher uses the statement text):
//!
//! - `read` for everything it evaluates — rule bodies, goal queries,
//!   `EXPLAIN RULE` / `ABDUCE`, `CALIBRATE` / `VALIDATE`, read-only Cypher
//!   commands — when it has any;
//! - one request per command that writes, under the verb Cypher would give
//!   that statement ([`crate::api::transaction::procedure_verb`]): `write`
//!   for a Cypher write and for a `DERIVE`, `schema` for a DDL procedure,
//!   `dbms` for an administrative one. The mutations of an `ASSUME` block
//!   count, as a Cypher write does whether or not its transaction commits.
//!
//! Each request carries the structured fields (labels, relationship types,
//! properties, operations) of the parts it covers. A `DERIVE` request names
//! what the equivalent Cypher write would: the patterns the rule matches —
//! through every rule it reaches by `IS`, since those bind what it writes —
//! as well as what it creates. A rule it cannot resolve refuses the program
//! rather than send a request that names nothing.

use std::collections::HashSet;

use uni_common::{Result, UniError};
use uni_cypher::ast::{Query, Statement};
use uni_cypher::locy_ast::{DeriveClause, RuleCondition, RuleOutput};
use uni_locy::CompiledProgram;
use uni_locy::types::{CompiledCommand, Stratum};
use uni_plugin::traits::connector::Principal;
use uni_plugin::traits::procedure::ProcedureMode;

use crate::api::UniInner;
use crate::api::session::{AuthzFields, authorize_resource};
use crate::api::transaction::{classify_verb, procedure_verb};

/// Authorizes running `compiled`, whose source text is `program`, for
/// `principal` (anonymous when `None`).
///
/// A no-op when no policy is registered.
///
/// # Errors
///
/// [`uni_common::UniError::AuthorizationDenied`] for the first request a
/// policy refuses.
pub(crate) fn authorize_locy_program(
    db: &UniInner,
    principal: Option<&Principal>,
    program: &str,
    compiled: &CompiledProgram,
) -> Result<()> {
    if db.plugin_registry.authz_policies().is_empty() {
        return Ok(());
    }
    let registered_mode = |name: &str| registered_mode(db, name);
    let mut requests = Requests::default();
    requests.add_strata(&compiled.strata);
    requests.add_commands(
        &compiled.commands,
        &[compiled],
        classify_verb(program),
        &registered_mode,
    )?;

    if requests.reads_anything {
        let mut fields = requests.reads;
        fields.add_operation("read");
        authorize_resource(db, principal, "read", program, fields)?;
    }
    for (verb, fields) in requests.writes {
        authorize_resource(db, principal, verb, program, fields)?;
    }
    Ok(())
}

/// Authorizes `tx.apply` replaying `queries`, the statements of a
/// `DerivedFactSet`, for `principal`, before any of them runs.
///
/// The set is plain data: it may come from another principal's session, or
/// be built by hand. So its statements are authorized where they execute, one
/// request per statement as Cypher makes, under the path `<locy-apply>` the
/// replay runs them with. A schema command authorizes as `schema`.
///
/// # Errors
///
/// [`uni_common::UniError::AuthorizationDenied`] for the first statement a
/// policy refuses.
pub(crate) fn authorize_replay(
    db: &UniInner,
    principal: Option<&Principal>,
    queries: &[Query],
) -> Result<()> {
    if db.plugin_registry.authz_policies().is_empty() {
        return Ok(());
    }
    let registered_mode = |name: &str| registered_mode(db, name);
    for query in queries {
        let mut fields = AuthzFields::default();
        fields.add_query(query);
        let verb = cypher_command_verb(query, "schema", &registered_mode);
        authorize_resource(db, principal, verb, "<locy-apply>", fields)?;
    }
    Ok(())
}

/// Mode of a registered (non-built-in) procedure, from the db-level registry.
fn registered_mode(db: &UniInner, name: &str) -> Option<ProcedureMode> {
    uni_plugin::QName::parse(name)
        .ok()
        .and_then(|q| db.plugin_registry.procedure(&q))
        .map(|e| e.signature.mode)
}

/// The authorization requests a program needs, gathered before any is made.
#[derive(Default)]
struct Requests {
    /// Whether the program evaluates anything (a rule or a reading command).
    reads_anything: bool,
    /// What the `read` request names.
    reads: AuthzFields,
    /// One request per writing command, in program order.
    writes: Vec<(&'static str, AuthzFields)>,
}

impl Requests {
    fn add_strata(&mut self, strata: &[Stratum]) {
        for rule in strata.iter().flat_map(|s| &s.rules) {
            self.reads_anything = true;
            for clause in &rule.clauses {
                self.reads.add_pattern(&clause.match_pattern);
            }
        }
    }

    /// Adds `commands`, compiled within `scopes` — the program that holds
    /// them, then each program enclosing it, innermost first. `schema_verb` is
    /// the verb of a schema command among them (see [`cypher_command_verb`]).
    ///
    /// # Errors
    ///
    /// [`UniError::AuthorizationDenied`] for a `DERIVE` whose rules cannot be
    /// resolved ([`add_derived`]).
    fn add_commands(
        &mut self,
        commands: &[CompiledCommand],
        scopes: &[&CompiledProgram],
        schema_verb: &'static str,
        registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
    ) -> Result<()> {
        for command in commands {
            match command {
                CompiledCommand::Cypher(query) => {
                    let verb = cypher_command_verb(query, schema_verb, registered_mode);
                    self.add_statement(query, verb);
                }
                CompiledCommand::DeriveCommand(derive) => {
                    // Evaluates the rule, then writes what it derives.
                    self.reads_anything = true;
                    let mut fields = AuthzFields::default();
                    fields.add_operation("read");
                    fields.add_operation("write");
                    add_derived(&mut fields, scopes, &derive.rule_name.to_string())?;
                    self.writes.push(("write", fields));
                }
                CompiledCommand::Assume(assume) => {
                    let mutations = Query::Single(Statement {
                        clauses: assume.mutations.clone(),
                    });
                    let verb = cypher_command_verb(&mutations, schema_verb, registered_mode);
                    self.add_statement(&mutations, verb);
                    self.add_strata(&assume.body_program.strata);
                    let mut inner = vec![&assume.body_program];
                    inner.extend_from_slice(scopes);
                    self.add_commands(&assume.body_commands, &inner, schema_verb, registered_mode)?;
                }
                CompiledCommand::Calibrate(c) => {
                    self.reads_anything = true;
                    self.reads.add_pattern(&c.pattern);
                }
                CompiledCommand::Validate(v) => {
                    self.reads_anything = true;
                    self.reads.add_pattern(&v.pattern);
                }
                CompiledCommand::GoalQuery(_)
                | CompiledCommand::Abduce(_)
                | CompiledCommand::ExplainRule(_) => self.reads_anything = true,
            }
        }
        Ok(())
    }

    /// Adds a Cypher statement the program runs under `verb`: into the
    /// `read` request when it reads, as a request of its own otherwise.
    fn add_statement(&mut self, query: &Query, verb: &'static str) {
        if verb == "read" {
            self.reads_anything = true;
            self.reads.add_query(query);
        } else {
            let mut fields = AuthzFields::default();
            fields.add_query(query);
            self.writes.push((verb, fields));
        }
    }
}

/// Adds what a `DERIVE` of the rule `name` touches: the patterns it and every
/// rule it reaches by `IS` match, as the `MATCH` of a Cypher write counts in
/// that write's request, and the labels, relationship types and inline
/// property keys it creates — `MERGED_WITH` for `DERIVE MERGE`.
///
/// A rule is looked up in each of `scopes` in turn.
///
/// # Errors
///
/// [`UniError::AuthorizationDenied`] when a rule cannot be resolved: a request
/// that named nothing would let a label-keyed policy allow the write.
fn add_derived(fields: &mut AuthzFields, scopes: &[&CompiledProgram], name: &str) -> Result<()> {
    let mut seen = HashSet::new();
    let mut pending = vec![name.to_owned()];
    while let Some(name) = pending.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let rule = scopes
            .iter()
            .find_map(|p| p.rule_catalog.get(&name))
            .ok_or_else(|| UniError::AuthorizationDenied {
                reason: format!("cannot authorize DERIVE: rule '{name}' is not resolvable"),
            })?;
        for clause in &rule.clauses {
            fields.add_pattern(&clause.match_pattern);
            for cond in &clause.where_conditions {
                if let RuleCondition::IsReference(r) = cond {
                    pending.push(r.rule_name.to_string());
                }
            }
            match &clause.output {
                RuleOutput::Derive(DeriveClause::Patterns(patterns)) => {
                    for p in patterns {
                        for node in [&p.source, &p.target] {
                            for label in &node.labels {
                                fields.add_label(label);
                            }
                            fields.add_map_keys(node.properties.as_ref());
                        }
                        fields.add_rel_type(&p.edge.edge_type);
                        fields.add_map_keys(p.edge.properties.as_ref());
                    }
                }
                RuleOutput::Derive(DeriveClause::Merge(..)) => fields.add_rel_type("MERGED_WITH"),
                RuleOutput::Yield(_) => {}
            }
        }
    }
    Ok(())
}

/// The verb Cypher's `tx.query` would authorize `query` under: `read` for a
/// read-only statement, otherwise [`procedure_verb`] over the statement's
/// [`classify_verb`] verb.
///
/// `classify_verb` reads every clause statement that writes as `write`. For a
/// schema command (`CREATE INDEX …`) the caller passes `schema_verb`: a
/// program passes `classify_verb` of its text, which for a schema command is
/// that statement — the grammar takes one only as a whole program.
fn cypher_command_verb(
    query: &Query,
    schema_verb: &'static str,
    registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
) -> &'static str {
    let is_write_procedure =
        |name: &str| registered_mode(name).is_some_and(|m| !matches!(m, ProcedureMode::Read));
    if uni_query::validate_read_only_with(query, &is_write_procedure).is_ok() {
        return "read";
    }
    let classified = match query {
        Query::Schema(_) => schema_verb,
        _ => "write",
    };
    procedure_verb(
        uni_query::strongest_procedure_mode(query, registered_mode),
        classified,
    )
}
