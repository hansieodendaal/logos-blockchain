use lb_http_api_common::paths::{MAX_BLOCKS_STREAM_BLOCKS, MAX_BLOCKS_STREAM_CHUNK_SIZE};
use serde::Deserialize;
use utoipa::IntoParams;
use validator::Validate;

#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[derive(Deserialize)]
pub struct BlockRangeQuery {
    #[param(minimum = 0)]
    pub slot_from: usize,
    #[param(minimum = 0)]
    pub slot_to: usize,
}

#[derive(IntoParams, Deserialize, Validate)]
#[into_params(parameter_in = Query)]
pub struct BlocksStreamQuery {
    /// If omitted, the server chooses a default lower bound.
    /// For descending streams this means no explicit lower bound.
    /// For ascending streams ending above LIB, the lower bound may be estimated
    /// from recent chain spacing and `blocks_limit`, so fewer than
    /// `blocks_limit` blocks may be returned.
    #[serde(default)]
    #[param(minimum = 0)]
    pub slot_from: Option<u64>,
    /// Upper bound slot (inclusive). Defaults to tip slot, or LIB slot when
    /// `immutable_only=true`.
    #[serde(default)]
    #[param(minimum = 0)]
    pub slot_to: Option<u64>,
    /// Sort direction. Defaults to descending (`true`).
    #[serde(default)]
    pub descending: Option<bool>,
    /// Maximum number of blocks to return. Defaults to `100`, maximum
    /// `630_720_000`.
    #[serde(default)]
    #[validate(custom(function = "validate_blocks_limit"))]
    #[param(minimum = 1, maximum = 630_720_000, default = 100, example = 100)]
    pub blocks_limit: Option<usize>,
    /// Server chunk size hint for streamed delivery. Defaults to `100` ,
    /// maximum `1000`.
    #[serde(default)]
    #[validate(custom(function = "validate_server_batch_size"))]
    #[param(minimum = 1, maximum = 1_000, default = 100, example = 100)]
    pub server_batch_size: Option<usize>,
    /// When true, include only immutable blocks.
    /// If `slot_to` is omitted, the default anchor is LIB slot.
    #[serde(default)]
    pub immutable_only: Option<bool>,
}

fn validate_blocks_limit(v: usize) -> Result<(), validator::ValidationError> {
    if v == 0 || v > MAX_BLOCKS_STREAM_BLOCKS {
        return Err(validator::ValidationError::new("blocks_limit_out_of_range"));
    }
    Ok(())
}

fn validate_server_batch_size(v: usize) -> Result<(), validator::ValidationError> {
    if v == 0 || v > MAX_BLOCKS_STREAM_CHUNK_SIZE {
        return Err(validator::ValidationError::new(
            "server_batch_size_out_of_range",
        ));
    }
    Ok(())
}
