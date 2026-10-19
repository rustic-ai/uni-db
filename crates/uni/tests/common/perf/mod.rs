pub mod anchored_lookup_test;
pub mod iai_wallclock;
pub mod l0_load_probe;
pub mod limit_pushdown_test;
pub mod perf_class_probe;
pub mod profiler_blind_spots;
pub mod query_deadline_preemption_test;
pub mod query_limits_test;
pub mod query_streaming_test;
pub mod stress_test;
pub mod test_100k_lookup;
pub mod test_bulk_insert_performance;
pub mod test_count_performance;
pub mod update_microbench_stress;
// `x IN fs` with `fs` a column repeated in runs (the IC5 shape).
pub mod in_list_column_runs;
