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
    #[serde(default)]
    #[param(minimum = 1)]
    pub number_of_blocks: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_blocks_to_header_id")]
    pub blocks_to: Option<HeaderId>,
    #[serde(default)]
    #[param(minimum = 1)]
    pub server_batch_size: Option<usize>,
    #[serde(default)]
    pub immutable_only: Option<bool>,
}
