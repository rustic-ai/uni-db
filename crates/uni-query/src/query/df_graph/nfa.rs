use std::collections::HashSet;
use uni_store::storage::direction::Direction;

/// NFA state identifier.
pub type NfaStateId = u16;

/// Default maximum hops for unbounded VLP patterns (`*` without upper bound).
pub const DEFAULT_MAX_HOPS: usize = 128;

/// Path traversal semantics controlling which paths are valid.
#[derive(Clone, Debug, PartialEq)]
pub enum PathMode {
    /// No restrictions on repeated edges or nodes.
    Walk,
    /// No repeated edges (OpenCypher default for VLP).
    Trail,
    /// No repeated nodes.
    Acyclic,
    /// No repeated nodes except start may equal end.
    Simple,
}

/// Selects which subset of matching paths to return.
#[derive(Clone, Debug)]
pub enum PathSelector {
    /// All matching paths (default).
    All,
    /// One arbitrary path per endpoint pair.
    Any,
    /// One shortest path per endpoint pair.
    AnyShortest,
    /// All shortest paths per endpoint pair.
    AllShortest,
    /// K shortest paths per endpoint pair.
    ShortestK(usize),
}

/// Determines the BFS strategy based on how the VLP result is consumed.
#[derive(Clone, Debug)]
pub enum VlpOutputMode {
    /// No path_variable, no step_variable — only endpoints and hop count.
    EndpointsOnly,
    /// Only `length(p)` or `min/max(length(p))` is used.
    LengthOnly { needs_max: bool },
    /// Only `count(p)` is used.
    CountOnly,
    /// Path variable is used in RETURN.
    FullPath,
    /// Step variable is bound (e.g., `[r*1..3]`).
    StepVariable,
    /// `shortestPath()` or `allShortestPaths()`.
    ShortestPath { selector: PathSelector },
    /// EXISTS pattern with VLP.
    Existential,
}

/// Constraint on a vertex at a given NFA state (for QPP intermediate nodes).
///
/// V1: label-only constraint checked via L0 visibility (O(1) for cached lookups).
/// Future: multi-label, property predicates, WHERE clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum VertexConstraint {
    /// Vertex must have this label.
    Label(String),
}

/// One step (hop) in a Quantified Path Pattern sub-pattern.
#[derive(Clone, Debug)]
pub struct QppStep {
    pub edge_type_ids: Vec<u32>,
    pub direction: Direction,
    pub target_constraint: Option<VertexConstraint>,
}

/// A single NFA transition between states.
#[derive(Clone, Debug)]
pub struct NfaTransition {
    pub from: NfaStateId,
    pub to: NfaStateId,
    pub edge_type_ids: Vec<u32>,
    pub direction: Direction,
}

/// Non-deterministic finite automaton for variable-length path matching.
///
/// For a simple VLP pattern `[:TYPE*min..max]`, this is a linear chain:
/// ```text
/// q0 --TYPE--> q1 --TYPE--> q2 --TYPE--> ... --TYPE--> q(max)
///              ^                                        ^
///              accepting if >= min_hops                  accepting
/// ```
///
/// For a QPP pattern `((a)-[:T1]->(b:Person)-[:T2]->(c)){2,4}`:
/// ```text
/// q0 --T1--> q1[Person] --T2--> q2 --T1--> q3[Person] --T2--> q4 --T1--> ...
///                                ^                              ^
///                                accepting (1 iter)             accepting (2 iter)
/// ```
/// State constraints are checked during BFS expansion.
#[derive(Clone, Debug)]
pub struct PathNfa {
    transitions: Vec<NfaTransition>,
    accepting_states: HashSet<NfaStateId>,
    start_state: NfaStateId,
    num_states: u16,
    /// Index into `transitions`: `transitions_by_state[state] = (start_idx, end_idx)`.
    transitions_by_state: Vec<(usize, usize)>,
    /// Per-state vertex constraint. `state_constraints[i]` is the constraint
    /// that must hold on a vertex reaching NFA state `i`. `None` = no constraint.
    state_constraints: Vec<Option<VertexConstraint>>,
}

impl PathNfa {
    /// Compile a simple VLP pattern into a linear-chain NFA.
    ///
    /// Creates states `q0..q(max_hops)`, with transitions from `qi` to `qi+1`.
    /// Accepting states are `q(min_hops)..=q(max_hops)`.
    pub fn from_vlp(
        edge_type_ids: Vec<u32>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
    ) -> Self {
        // Gracefully handle empty intervals (min > max) — return a trivial NFA
        // with no accepting states. The BFS will find nothing and return 0 results.
        if min_hops > max_hops {
            return Self {
                transitions: Vec::new(),
                accepting_states: HashSet::new(),
                start_state: 0,
                num_states: 1,
                transitions_by_state: vec![(0, 0)],
                state_constraints: vec![None],
            };
        }

        let num_states = (max_hops + 1) as u16;

        // Build transitions: q0->q1, q1->q2, ..., q(max-1)->q(max)
        let mut transitions = Vec::with_capacity(max_hops);
        for i in 0..max_hops {
            transitions.push(NfaTransition {
                from: i as NfaStateId,
                to: (i + 1) as NfaStateId,
                edge_type_ids: edge_type_ids.clone(),
                direction,
            });
        }

        // Accepting states: q(min)..=q(max)
        let accepting_states: HashSet<NfaStateId> =
            (min_hops..=max_hops).map(|s| s as NfaStateId).collect();

        // Build transitions_by_state index.
        // State i (where i < max_hops) has exactly one transition at index i.
        // State max_hops has no outgoing transitions.
        let mut transitions_by_state = Vec::with_capacity(num_states as usize);
        for i in 0..num_states {
            if (i as usize) < max_hops {
                transitions_by_state.push((i as usize, i as usize + 1));
            } else {
                let len = transitions.len();
                transitions_by_state.push((len, len));
            }
        }

        // No state constraints for simple VLP patterns.
        let state_constraints = vec![None; num_states as usize];

        Self {
            transitions,
            accepting_states,
            start_state: 0,
            num_states,
            transitions_by_state,
            state_constraints,
        }
    }

    /// Get all transitions from a given NFA state.
    pub fn transitions_from(&self, state: NfaStateId) -> &[NfaTransition] {
        if (state as usize) < self.transitions_by_state.len() {
            let (start, end) = self.transitions_by_state[state as usize];
            &self.transitions[start..end]
        } else {
            &[]
        }
    }

    /// Check if a state is an accepting state.
    pub fn is_accepting(&self, state: NfaStateId) -> bool {
        self.accepting_states.contains(&state)
    }

    /// Get the start state of the NFA.
    pub fn start_state(&self) -> NfaStateId {
        self.start_state
    }

    /// Get the total number of states.
    pub fn num_states(&self) -> u16 {
        self.num_states
    }

    /// Get a reference to the accepting states set.
    pub fn accepting_states(&self) -> &HashSet<NfaStateId> {
        &self.accepting_states
    }

    /// Get the state constraint for a given NFA state.
    pub fn state_constraint(&self, state: NfaStateId) -> Option<&VertexConstraint> {
        self.state_constraints
            .get(state as usize)
            .and_then(|c| c.as_ref())
    }

    /// Compile a QPP (Quantified Path Pattern) into an NFA with per-state constraints.
    ///
    /// `steps`: the sub-pattern hops (e.g., 2 steps for `(a)-[:T1]->(b:Person)-[:T2]->(c)`)
    /// `min_iterations`, `max_iterations`: quantifier bounds (iterations, NOT hops)
    ///
    /// For a 2-step sub-pattern with `{2,4}`:
    /// - Total hops = 2 * 4 = 8
    /// - States: q0, q1, q2, q3, q4, q5, q6, q7, q8 (9 states)
    /// - Accepting at iteration boundaries: q4, q6, q8 (iterations 2, 3, 4)
    /// - State constraints: step\[0\].target_constraint at q1, q3, q5, q7;
    ///   step\[1\].target_constraint at q2, q4, q6, q8
    pub fn from_qpp(steps: Vec<QppStep>, min_iterations: usize, max_iterations: usize) -> Self {
        assert!(!steps.is_empty(), "QPP must have at least one step");

        // Gracefully handle empty intervals (min > max) — return a trivial NFA
        // with no accepting states.
        if min_iterations > max_iterations {
            return Self {
                transitions: Vec::new(),
                accepting_states: HashSet::new(),
                start_state: 0,
                num_states: 1,
                transitions_by_state: vec![(0, 0)],
                state_constraints: vec![None],
            };
        }

        let hops_per_iter = steps.len();
        let total_hops = hops_per_iter * max_iterations;
        let num_states = (total_hops + 1) as u16;

        // Build transitions: one per hop, cycling through steps
        let mut transitions = Vec::with_capacity(total_hops);
        let mut state_constraints = vec![None; num_states as usize];

        for hop in 0..total_hops {
            let step = &steps[hop % hops_per_iter];
            let from = hop as NfaStateId;
            let to = (hop + 1) as NfaStateId;

            transitions.push(NfaTransition {
                from,
                to,
                edge_type_ids: step.edge_type_ids.clone(),
                direction: step.direction,
            });

            // Apply the step's target constraint to the destination state
            if let Some(ref constraint) = step.target_constraint {
                state_constraints[to as usize] = Some(constraint.clone());
            }
        }

        // Accepting states at iteration boundaries >= min_iterations
        let mut accepting_states = HashSet::new();
        for iter in min_iterations..=max_iterations {
            accepting_states.insert((iter * hops_per_iter) as NfaStateId);
        }

        // Build transitions_by_state index (same structure as from_vlp: linear chain)
        let mut transitions_by_state = Vec::with_capacity(num_states as usize);
        for i in 0..num_states {
            if (i as usize) < total_hops {
                transitions_by_state.push((i as usize, i as usize + 1));
            } else {
                let len = transitions.len();
                transitions_by_state.push((len, len));
            }
        }

        Self {
            transitions,
            accepting_states,
            start_state: 0,
            num_states,
            transitions_by_state,
            state_constraints,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vlp_to_nfa_basic() {
        // [:KNOWS*2..5] → 6 states (q0..q5), accepting {q2, q3, q4, q5}
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 2, 5);
        assert_eq!(nfa.num_states(), 6);
        assert_eq!(nfa.start_state(), 0);
        assert_eq!(nfa.transitions.len(), 5);
        assert_eq!(
            nfa.accepting_states(),
            &[2, 3, 4, 5].into_iter().collect::<HashSet<NfaStateId>>()
        );
        // VLP has no state constraints
        for i in 0..6 {
            assert!(nfa.state_constraint(i).is_none());
        }
    }

    #[test]
    fn test_vlp_to_nfa_unbounded() {
        // [:KNOWS*] → equivalent to *1..DEFAULT_MAX_HOPS
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 1, DEFAULT_MAX_HOPS);
        assert_eq!(nfa.num_states(), (DEFAULT_MAX_HOPS + 1) as u16);
        assert!(!nfa.is_accepting(0));
        assert!(nfa.is_accepting(1));
        assert!(nfa.is_accepting(DEFAULT_MAX_HOPS as NfaStateId));
        assert_eq!(nfa.transitions.len(), DEFAULT_MAX_HOPS);
    }

    #[test]
    fn test_vlp_to_nfa_zero_min() {
        // [:KNOWS*0..3] → 4 states, q0 IS accepting
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 0, 3);
        assert_eq!(nfa.num_states(), 4);
        assert!(nfa.is_accepting(0));
        assert!(nfa.is_accepting(1));
        assert!(nfa.is_accepting(2));
        assert!(nfa.is_accepting(3));
    }

    #[test]
    fn test_vlp_to_nfa_exact() {
        // [:KNOWS*3] → 4 states, only q3 is accepting
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 3, 3);
        assert_eq!(nfa.num_states(), 4);
        assert!(!nfa.is_accepting(0));
        assert!(!nfa.is_accepting(1));
        assert!(!nfa.is_accepting(2));
        assert!(nfa.is_accepting(3));
    }

    #[test]
    fn test_vlp_to_nfa_multi_type() {
        // [:KNOWS|LIKES*1..3] → transitions carry both type IDs
        let nfa = PathNfa::from_vlp(vec![1, 2], Direction::Outgoing, 1, 3);
        assert_eq!(nfa.num_states(), 4);
        assert_eq!(nfa.transitions.len(), 3);
        for t in &nfa.transitions {
            assert_eq!(t.edge_type_ids, vec![1, 2]);
        }
    }

    #[test]
    fn test_vlp_to_nfa_direction_both() {
        // Undirected VLP: all transitions carry Direction::Both
        let nfa = PathNfa::from_vlp(vec![1], Direction::Both, 1, 2);
        assert_eq!(nfa.num_states(), 3);
        for t in &nfa.transitions {
            assert_eq!(t.direction, Direction::Both);
        }
    }

    #[test]
    fn test_nfa_transitions_from() {
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 1, 3);
        // q0 has one outgoing transition (q0->q1)
        let t0 = nfa.transitions_from(0);
        assert_eq!(t0.len(), 1);
        assert_eq!(t0[0].from, 0);
        assert_eq!(t0[0].to, 1);

        // q2 has one outgoing transition (q2->q3)
        let t2 = nfa.transitions_from(2);
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].from, 2);
        assert_eq!(t2[0].to, 3);

        // q3 (max state) has no outgoing transitions
        let t3 = nfa.transitions_from(3);
        assert_eq!(t3.len(), 0);

        // Out-of-range state returns empty slice
        let t99 = nfa.transitions_from(99);
        assert_eq!(t99.len(), 0);
    }

    #[test]
    fn test_nfa_is_accepting() {
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 2, 4);
        assert!(!nfa.is_accepting(0));
        assert!(!nfa.is_accepting(1));
        assert!(nfa.is_accepting(2));
        assert!(nfa.is_accepting(3));
        assert!(nfa.is_accepting(4));
        assert!(!nfa.is_accepting(5)); // doesn't exist
    }

    // --- QPP tests ---

    #[test]
    fn test_qpp_two_hop_basic() {
        // ((a)-[:T1]->(b)-[:T2]->(c)){1,3}
        // 2 steps × 3 max iterations = 6 total hops, 7 states
        // Accepting at iteration boundaries: 1*2=2, 2*2=4, 3*2=6
        let steps = vec![
            QppStep {
                edge_type_ids: vec![1],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
            QppStep {
                edge_type_ids: vec![2],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
        ];
        let nfa = PathNfa::from_qpp(steps, 1, 3);

        assert_eq!(nfa.num_states(), 7);
        assert_eq!(nfa.transitions.len(), 6);

        // Accepting at q2, q4, q6
        assert!(!nfa.is_accepting(0));
        assert!(!nfa.is_accepting(1));
        assert!(nfa.is_accepting(2));
        assert!(!nfa.is_accepting(3));
        assert!(nfa.is_accepting(4));
        assert!(!nfa.is_accepting(5));
        assert!(nfa.is_accepting(6));
    }

    #[test]
    fn test_qpp_state_constraints() {
        // ((a)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)){1,2}
        // Step 0: KNOWS, target=Person (at odd states: 1, 3)
        // Step 1: WORKS_AT, target=Company (at even states: 2, 4)
        let steps = vec![
            QppStep {
                edge_type_ids: vec![10],
                direction: Direction::Outgoing,
                target_constraint: Some(VertexConstraint::Label("Person".to_string())),
            },
            QppStep {
                edge_type_ids: vec![20],
                direction: Direction::Outgoing,
                target_constraint: Some(VertexConstraint::Label("Company".to_string())),
            },
        ];
        let nfa = PathNfa::from_qpp(steps, 1, 2);

        assert_eq!(nfa.num_states(), 5); // 2*2 + 1 = 5

        // State constraints: q1=Person, q2=Company, q3=Person, q4=Company
        assert!(nfa.state_constraint(0).is_none());
        assert_eq!(
            nfa.state_constraint(1),
            Some(&VertexConstraint::Label("Person".to_string()))
        );
        assert_eq!(
            nfa.state_constraint(2),
            Some(&VertexConstraint::Label("Company".to_string()))
        );
        assert_eq!(
            nfa.state_constraint(3),
            Some(&VertexConstraint::Label("Person".to_string()))
        );
        assert_eq!(
            nfa.state_constraint(4),
            Some(&VertexConstraint::Label("Company".to_string()))
        );
    }

    #[test]
    fn test_qpp_transitions_alternate() {
        // ((a)-[:T1]->(b)-[:T2]->(c)){1,2}
        // Transitions should alternate: T1, T2, T1, T2
        let steps = vec![
            QppStep {
                edge_type_ids: vec![1],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
            QppStep {
                edge_type_ids: vec![2],
                direction: Direction::Incoming,
                target_constraint: None,
            },
        ];
        let nfa = PathNfa::from_qpp(steps, 1, 2);

        assert_eq!(nfa.transitions.len(), 4);
        assert_eq!(nfa.transitions[0].edge_type_ids, vec![1]);
        assert_eq!(nfa.transitions[0].direction, Direction::Outgoing);
        assert_eq!(nfa.transitions[1].edge_type_ids, vec![2]);
        assert_eq!(nfa.transitions[1].direction, Direction::Incoming);
        assert_eq!(nfa.transitions[2].edge_type_ids, vec![1]);
        assert_eq!(nfa.transitions[2].direction, Direction::Outgoing);
        assert_eq!(nfa.transitions[3].edge_type_ids, vec![2]);
        assert_eq!(nfa.transitions[3].direction, Direction::Incoming);
    }

    #[test]
    fn test_qpp_single_hop_equiv() {
        // ((a)-[:T]->(b)){2,4} should be equivalent to [:T*2..4]
        let qpp_steps = vec![QppStep {
            edge_type_ids: vec![1],
            direction: Direction::Outgoing,
            target_constraint: None,
        }];
        let qpp_nfa = PathNfa::from_qpp(qpp_steps, 2, 4);
        let vlp_nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 2, 4);

        assert_eq!(qpp_nfa.num_states(), vlp_nfa.num_states());
        assert_eq!(qpp_nfa.accepting_states(), vlp_nfa.accepting_states());
        assert_eq!(qpp_nfa.transitions.len(), vlp_nfa.transitions.len());
    }

    #[test]
    fn test_qpp_accepting_at_boundaries() {
        // ((a)-[:T1]->(b)-[:T2]->(c)-[:T3]->(d)){2,3}
        // 3 steps, accepting at 2*3=6 and 3*3=9
        let steps = vec![
            QppStep {
                edge_type_ids: vec![1],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
            QppStep {
                edge_type_ids: vec![2],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
            QppStep {
                edge_type_ids: vec![3],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
        ];
        let nfa = PathNfa::from_qpp(steps, 2, 3);

        assert_eq!(nfa.num_states(), 10); // 3*3 + 1

        // Only q6 and q9 are accepting
        for i in 0..10u16 {
            if i == 6 || i == 9 {
                assert!(nfa.is_accepting(i), "State {i} should be accepting");
            } else {
                assert!(!nfa.is_accepting(i), "State {i} should not be accepting");
            }
        }
    }

    #[test]
    fn test_qpp_zero_min() {
        // ((a)-[:T]->(b)){0,3} — zero iterations makes start state accepting
        let steps = vec![QppStep {
            edge_type_ids: vec![1],
            direction: Direction::Outgoing,
            target_constraint: None,
        }];
        let nfa = PathNfa::from_qpp(steps, 0, 3);

        assert!(nfa.is_accepting(0)); // Zero iterations
        assert!(nfa.is_accepting(1)); // 1 iteration
        assert!(nfa.is_accepting(2)); // 2 iterations
        assert!(nfa.is_accepting(3)); // 3 iterations
    }

    #[test]
    fn test_qpp_unbounded_capped() {
        // ((a)-[:T1]->(b)-[:T2]->(c)){2,} — cap at DEFAULT_MAX_HOPS / hops_per_iter
        let steps = vec![
            QppStep {
                edge_type_ids: vec![1],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
            QppStep {
                edge_type_ids: vec![2],
                direction: Direction::Outgoing,
                target_constraint: None,
            },
        ];
        let max_iter = DEFAULT_MAX_HOPS / steps.len();
        let nfa = PathNfa::from_qpp(steps, 2, max_iter);

        let expected_states = (max_iter * 2 + 1) as u16;
        assert_eq!(nfa.num_states(), expected_states);
        assert!(nfa.is_accepting((2 * 2) as NfaStateId)); // min: 2 iterations
        assert!(nfa.is_accepting((max_iter * 2) as NfaStateId)); // max
        assert!(!nfa.is_accepting(1)); // Not at iteration boundary
    }

    #[test]
    fn test_vlp_empty_interval_no_panic() {
        // [*3..1] is an empty interval — min > max
        // Should NOT panic, should produce an NFA with 0 accepting states
        let nfa = PathNfa::from_vlp(vec![1], Direction::Outgoing, 3, 1);
        assert!(
            nfa.accepting_states().is_empty(),
            "Empty interval NFA should have no accepting states"
        );
        // The NFA should be well-formed (1 state, no transitions)
        assert_eq!(nfa.num_states(), 1);
        assert_eq!(nfa.start_state(), 0);
    }

    #[test]
    fn test_qpp_empty_interval_no_panic() {
        // QPP with min_iterations > max_iterations — should not panic
        let steps = vec![QppStep {
            edge_type_ids: vec![1],
            direction: Direction::Outgoing,
            target_constraint: None,
        }];
        let nfa = PathNfa::from_qpp(steps, 3, 1);
        assert!(
            nfa.accepting_states().is_empty(),
            "Empty interval QPP NFA should have no accepting states"
        );
        assert_eq!(nfa.num_states(), 1);
    }

    #[test]
    fn test_vertex_constraint_label() {
        let c = VertexConstraint::Label("Person".to_string());
        assert_eq!(c, VertexConstraint::Label("Person".to_string()));
        assert_ne!(c, VertexConstraint::Label("Company".to_string()));
    }
}
