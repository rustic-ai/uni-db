pub mod error;
pub mod result;

pub use error::{match_error, ErrorPhase, TckErrorType};
pub use result::{
    match_result, match_result_ignoring_list_order, match_result_unordered,
    match_result_unordered_ignoring_list_order,
};
