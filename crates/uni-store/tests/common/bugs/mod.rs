pub mod test_issue_112_transaction_edge_versions;
pub mod test_issue_143_oom_guards;
pub mod test_issue_18_150_poisoned_mutex;
pub mod test_issue_19_tx_memory_limit;
pub mod test_issue_211_vid_labels_index_rebuild;
pub mod test_issue_25_cascade_deletion;
pub mod test_issue_27_chunked_index;
pub mod test_issue_29_vid_labels_index;
pub mod test_issue_43_uid_index;
pub mod test_issue_4_constraint_check;
pub mod test_issue_53_edge_properties_after_compaction;
pub mod test_issue_54_adjacency_compaction_visibility;
pub mod test_issue_62_recursive_decoder;
pub mod test_issue_75_batch_frontier;
pub mod test_issue_77_ghost_vertex_endpoints;

// Correctness-scan Wave 0 fault-injection helpers (R2 error-swallowing).
pub mod fault_backend;
pub mod fault_store;

// Correctness-scan Wave 0 repros (R2 error-swallowing).
pub mod repro_12_fork_registry_orphan;
pub mod repro_14_snapshot_named_wipe;
pub mod repro_15_scan_table_exists_swallow;

// Correctness-scan Wave 0 repros (R3 tombstone resurrection).
pub mod repro_01_replay_label_overwrite_marker;
pub mod repro_11_compact_adjacency_empty_resurrect;
pub mod repro_17_merge_l0_vector_tombstone_resurrect;
pub mod repro_17_overlay_tombstone_ungated;
pub mod repro_18_merge_l0_fts_tombstone_resurrect;
pub mod repro_edge_props_lost_after_compaction;
pub mod test_repro_compaction_empty_resurrect;

// Correctness-scan Wave 1 repros (R4 MVCC batch version-ranking).
pub mod repro_03_batch_edge_props_version_ignored;
pub mod repro_07_batch_vertex_props_version_ignored;
pub mod repro_18_main_vertex_batch_resurrect;

// Correctness-scan Wave 1 repros (R5 constraint visibility).
pub mod repro_02_batch_insert_constraint_index;
pub mod repro_03_batch_constraints_skip_pending;

// Correctness-scan Wave 1 repros (R17 arrow null-on-missing builders).
pub mod repro_05_06_temporal_missing_not_null;

// Correctness-scan Wave 2 repros (L7 compaction / durability races).
pub mod repro_04_adjacency_incoming_shadow;
pub mod repro_10_id_allocator_persist_fail;
pub mod repro_16_compact_vertices_concurrent_flush;
pub mod repro_id_allocator_substring_notfound;

// Tier 1.10a — object-store error classification (typed vs. substring), and the
// classifier/attempt-budget ordering bug in ResilientObjectStore::retry.
pub mod repro_resilient_stringly_classifier;

// Tier 1.1 — the L1 main-edge property fallback read at HEAD while L0 and the
// delta tier were bounded by the snapshot.
pub mod repro_schemaless_edge_props_escape_pin;

// Tier 1.2 — shadow-CSR retention grew unbounded in the number of warms.
pub mod repro_shadow_csr_warm_retention;
