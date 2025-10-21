use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    pin::Pin,
};

use futures::{Stream, StreamExt as _};
use kzgrs_backend::common::share::{DaShare, DaSharesCommitments};
use libp2p_identity::PeerId;
use nomos_banning::BanningService;
use nomos_core::{block::SessionNumber, da::BlobId, header::HeaderId};
use nomos_da_network_core::SubnetworkId;
use nomos_da_network_service::{
    DaNetworkMsg, NetworkService,
    api::ApiAdapter as ApiAdapterTrait,
    backends::libp2p::{
        common::{HistoricSamplingEvent, SamplingEvent},
        validator::{
            DaNetworkEvent, DaNetworkEventKind, DaNetworkMessage, DaNetworkValidatorBackend,
        },
    },
    membership::{MembershipAdapter, handler::DaMembershipHandler},
};
use overwatch::{
    DynError,
    services::{AsServiceId, ServiceData, relay::OutboundRelay},
};
use subnetworks_assignations::MembershipHandler;
use tokio::sync::oneshot;

use crate::network::{CommitmentsEvent, NetworkAdapter, adapters::common::adapter_for};

adapter_for!(
    DaNetworkValidatorBackend,
    DaNetworkMessage,
    DaNetworkEventKind,
    DaNetworkEvent
);
