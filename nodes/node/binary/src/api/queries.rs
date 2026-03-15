use serde::Deserialize;
use utoipa::IntoParams;

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
pub struct ChannelInscriptionsQuery {
    #[param(minimum = 0)]
    pub slot_from: usize,
    #[param(minimum = 0)]
    pub slot_to: usize,
    pub include_inscription: Option<bool>,
}

#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[derive(Deserialize)]
pub struct ChannelInscriptionsStreamQuery {
    #[param(minimum = 0)]
    pub from_slot: Option<usize>,
    pub include_inscription: Option<bool>,
}
