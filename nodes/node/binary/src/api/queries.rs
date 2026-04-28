use lb_core::header::HeaderId;
use serde::Deserialize;
use utoipa::IntoParams;

fn deserialize_blocks_to_header_id<'de, D>(deserializer: D) -> Result<Option<HeaderId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            let value = value.strip_prefix("0x").unwrap_or(value.as_str());
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(value, &mut bytes).map_err(serde::de::Error::custom)?;
            Ok(HeaderId::from(bytes))
        })
        .transpose()
}

#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[derive(Deserialize)]
pub struct BlockRangeQuery {
    #[param(minimum = 0)]
    pub slot_from: usize,
    #[param(minimum = 0)]
    pub slot_to: usize,
}

#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[derive(Deserialize)]
pub struct BlocksStreamQuery {
    /// Number of blocks to return. Defaults to 100.
    #[serde(default)]
    #[param(minimum = 1)]
    pub number_of_blocks: Option<usize>,
    /// Upper-bound canonical header (hex, with or without 0x).
    /// If omitted, defaults to tip when `immutable_only=false`, or LIB when
    /// `immutable_only=true`.
    #[serde(default, deserialize_with = "deserialize_blocks_to_header_id")]
    pub blocks_to: Option<HeaderId>,
    /// Server chunk size hint for streamed delivery.
    #[serde(default)]
    #[param(minimum = 1)]
    pub server_batch_size: Option<usize>,
    /// When true, include only immutable blocks.
    /// If `blocks_to` is omitted, the default anchor is LIB.
    #[serde(default)]
    pub immutable_only: Option<bool>,
}
