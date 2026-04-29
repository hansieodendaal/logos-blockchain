use std::{collections::VecDeque, num::NonZero, sync::Arc};

use futures::{Stream, StreamExt as _};
pub use lb_chain_broadcast_service::BlockInfo;
pub use lb_chain_service::{CryptarchiaInfo, Slot, State};
use lb_core::{
    header::{ContentId, HeaderId},
    mantle::SignedMantleTx,
    proofs::leader_proof::Groth16LeaderProof,
};
use lb_groth16::fr_to_bytes;
use lb_http_api_common::{
    bodies::wallet::{
        balance::WalletBalanceResponseBody,
        transfer_funds::{WalletTransferFundsRequestBody, WalletTransferFundsResponseBody},
    },
    paths::{
        BLOCKS, BLOCKS_DETAIL, BLOCKS_STREAM, CRYPTARCHIA_INFO, CRYPTARCHIA_LIB_STREAM,
        MAX_BLOCKS_STREAM_BLOCKS, MAX_BLOCKS_STREAM_CHUNK_SIZE, MEMPOOL_ADD_TX,
        wallet::{BALANCE, TRANSACTIONS_TRANSFER_FUNDS},
    },
    settings::default_max_body_size,
};
use lb_key_management_system_keys::keys::ZkPublicKey;
use reqwest::{Client, ClientBuilder, RequestBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Client-side header representation matching the server's
/// `ApiHeaderSerializer`.
#[derive(Clone, Debug, Deserialize)]
pub struct ApiHeader {
    pub id: HeaderId,
    pub parent_block: HeaderId,
    pub slot: Slot,
    pub block_root: ContentId,
    pub proof_of_leadership: Groth16LeaderProof,
}

/// Client-side block representation matching the server's `ApiBlockSerializer`.
/// Note: The server omits the signature field.
#[derive(Clone, Debug, Deserialize)]
pub struct ApiBlock {
    pub header: ApiHeader,
    pub transactions: Vec<SignedMantleTx>,
}

/// Processed block event from the blocks stream.
/// Matches the server's `ApiProcessedBlockEvent`.
#[derive(Clone, Debug, Deserialize)]
pub struct ProcessedBlockEvent {
    pub block: ApiBlock,
    pub tip: HeaderId,
    pub tip_slot: Slot,
    pub lib: HeaderId,
    pub lib_slot: Slot,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Internal server error: {0}")]
    Server(String),
    #[error("Failed to execute request: {0}")]
    Client(String),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
}

#[derive(Clone)]
pub struct BasicAuthCredentials {
    username: String,
    password: Option<String>,
}

impl BasicAuthCredentials {
    #[must_use]
    pub const fn new(username: String, password: Option<String>) -> Self {
        Self { username, password }
    }
}

#[derive(Clone)]
pub struct CommonHttpClient {
    client: Arc<Client>,
    basic_auth: Option<BasicAuthCredentials>,
}

impl CommonHttpClient {
    #[must_use]
    pub fn new(basic_auth: Option<BasicAuthCredentials>) -> Self {
        let initial_stream_window_size: u32 =
            u32::try_from(6 * default_max_body_size() / 10).unwrap_or(4 * 1025);
        let client = ClientBuilder::new()
            .http2_initial_stream_window_size(initial_stream_window_size)
            .build()
            .expect("Client from default settings should be able to build");
        Self {
            client: Arc::new(client),
            basic_auth,
        }
    }

    #[must_use]
    pub fn new_with_client(client: Client, basic_auth: Option<BasicAuthCredentials>) -> Self {
        Self {
            client: Arc::new(client),
            basic_auth,
        }
    }

    pub async fn post<Req, Res>(&self, request_url: Url, request_body: &Req) -> Result<Res, Error>
    where
        Req: Serialize + ?Sized + Send + Sync,
        Res: DeserializeOwned + Send + Sync,
    {
        let request = self.client.post(request_url).json(request_body);
        self.execute_request::<Res>(request).await
    }

    pub async fn get<Req, Res>(
        &self,
        request_url: Url,
        request_body: Option<&Req>,
    ) -> Result<Res, Error>
    where
        Req: Serialize + ?Sized + Send + Sync,
        Res: DeserializeOwned + Send + Sync,
    {
        let mut request = self.client.get(request_url);
        if let Some(request_body) = request_body {
            request = request.json(request_body);
        }
        self.execute_request::<Res>(request).await
    }

    async fn execute_request<Res: DeserializeOwned>(
        &self,
        mut request: RequestBuilder,
    ) -> Result<Res, Error> {
        if let Some(basic_auth) = &self.basic_auth {
            request = request.basic_auth(&basic_auth.username, basic_auth.password.as_deref());
        }

        let response = request.send().await.map_err(Error::Request)?;
        let status = response.status();
        let body = response.text().await.map_err(Error::Request)?;

        match status {
            StatusCode::OK | StatusCode::CREATED => serde_json::from_str(&body)
                .map_err(|e| Error::Server(format!("Failed to parse response: {e}"))),
            StatusCode::INTERNAL_SERVER_ERROR => Err(Error::Server(body)),
            _ => Err(Error::Server(format!(
                "Unexpected response [{status}]: {body}",
            ))),
        }
    }

    pub async fn get_lib_stream(
        &self,
        base_url: Url,
    ) -> Result<impl Stream<Item = BlockInfo> + use<>, Error> {
        let request_url = base_url
            .join(CRYPTARCHIA_LIB_STREAM.trim_start_matches('/'))
            .map_err(Error::Url)?;
        let mut request = self.client.get(request_url);

        if let Some(basic_auth) = &self.basic_auth {
            request = request.basic_auth(&basic_auth.username, basic_auth.password.as_deref());
        }

        let response = request.send().await.map_err(Error::Request)?;
        let status = response.status();

        let lib_stream = response.bytes_stream().filter_map(async |item| {
            let bytes = item.ok()?;
            serde_json::from_slice::<BlockInfo>(&bytes).ok()
        });
        match status {
            StatusCode::OK => Ok(lib_stream),
            StatusCode::INTERNAL_SERVER_ERROR => Err(Error::Server("Error".to_owned())),
            _ => Err(Error::Server(format!("Unexpected response [{status}]"))),
        }
    }

    pub async fn get_block_by_id(
        &self,
        base_url: Url,
        id: HeaderId,
    ) -> Result<Option<ApiBlock>, Error> {
        let path = BLOCKS_DETAIL
            .trim_start_matches('/')
            .replace(":id", &id.to_string());
        let request_url = base_url.join(path.as_str()).map_err(Error::Url)?;

        let mut request = self.client.get(request_url);
        if let Some(basic_auth) = &self.basic_auth {
            request = request.basic_auth(&basic_auth.username, basic_auth.password.as_deref());
        }

        let response = request.send().await.map_err(Error::Request)?;
        let status = response.status();
        let body = response.text().await.map_err(Error::Request)?;

        match status {
            StatusCode::OK => serde_json::from_str::<ApiBlock>(&body)
                .map(Some)
                .map_err(|e| Error::Server(format!("Failed to parse response: {e}"))),
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::INTERNAL_SERVER_ERROR => Err(Error::Server(body)),
            _ => Err(Error::Server(format!(
                "Unexpected response [{status}]: {body}",
            ))),
        }
    }

    pub async fn post_transaction<Tx>(&self, base_url: Url, transaction: Tx) -> Result<(), Error>
    where
        Tx: Serialize + Send + Sync + 'static,
    {
        let request_url = base_url
            .join(MEMPOOL_ADD_TX.trim_start_matches('/'))
            .map_err(Error::Url)?;
        self.post(request_url, &transaction).await
    }

    /// Get consensus info (tip, height, etc.)
    pub async fn consensus_info(&self, base_url: Url) -> Result<CryptarchiaInfo, Error> {
        let request_url = base_url
            .join(CRYPTARCHIA_INFO.trim_start_matches('/'))
            .map_err(Error::Url)?;
        self.get::<(), CryptarchiaInfo>(request_url, None).await
    }

    /// Get blocks in a slot range.
    pub async fn get_immutable_blocks(
        &self,
        base_url: Url,
        slot_from: u64,
        slot_to: u64,
    ) -> Result<Vec<ApiBlock>, Error> {
        let mut request_url = base_url
            .join(BLOCKS.trim_start_matches('/'))
            .map_err(Error::Url)?;
        request_url
            .query_pairs_mut()
            .append_pair("slot_from", &slot_from.to_string())
            .append_pair("slot_to", &slot_to.to_string());
        self.get::<(), Vec<ApiBlock>>(request_url, None).await
    }

    /// Subscribe to the processed blocks stream.
    /// Each event contains the block, current tip, and current LIB.
    pub async fn get_blocks_stream(
        &self,
        base_url: Url,
    ) -> Result<impl Stream<Item = ProcessedBlockEvent> + use<>, Error> {
        self.get_blocks_stream_in_range(base_url, None, None, None, None)
            .await
    }

    /// Stream processed blocks within a slot range.
    pub async fn get_blocks_stream_in_range(
        &self,
        base_url: Url,
        blocks_limit: Option<NonZero<usize>>,
        slot_from: Option<u64>,
        slot_to: Option<u64>,
        descending: Option<bool>,
    ) -> Result<impl Stream<Item = ProcessedBlockEvent> + use<>, Error> {
        self.get_blocks_stream_in_range_with_server_batch_size(
            base_url,
            blocks_limit,
            slot_from,
            slot_to,
            descending,
            None,
            None,
        )
        .await
    }

    // Helper function to validate inputs for block streaming methods.
    fn verify_inputs(
        blocks_limit: Option<NonZero<usize>>,
        slot_from: Option<u64>,
        slot_to: Option<u64>,
        server_batch_size: Option<NonZero<usize>>,
    ) -> Result<(), Error> {
        if let Some(blocks) = blocks_limit
            && blocks.get() > MAX_BLOCKS_STREAM_BLOCKS
        {
            return Err(Error::Client(format!(
                "'blocks_limit' must be <= {MAX_BLOCKS_STREAM_BLOCKS}, got {blocks}"
            )));
        }
        if let Some(size) = server_batch_size
            && size.get() > MAX_BLOCKS_STREAM_CHUNK_SIZE
        {
            return Err(Error::Client(format!(
                "'server_batch_size' must be <= {MAX_BLOCKS_STREAM_CHUNK_SIZE}, got {size}"
            )));
        }
        if let (Some(slot_from), Some(slot_to)) = (slot_from, slot_to)
            && slot_from > slot_to
        {
            return Err(Error::Client(format!(
                "'slot_from' must be <= 'slot_to', got slot_from={slot_from}, slot_to={slot_to}"
            )));
        }

        Ok(())
    }

    /// Stream processed blocks in a slot-bounded window.
    ///
    /// `server_batch_size` lets callers request smaller chunks; the server
    /// still enforces its own upper bound.
    #[expect(clippy::too_many_arguments, reason = "Need all args")]
    pub async fn get_blocks_stream_in_range_with_server_batch_size(
        &self,
        base_url: Url,
        blocks_limit: Option<NonZero<usize>>,
        slot_from: Option<u64>,
        slot_to: Option<u64>,
        descending: Option<bool>,
        server_batch_size: Option<NonZero<usize>>,
        immutable_only: Option<bool>,
    ) -> Result<impl Stream<Item = ProcessedBlockEvent> + use<>, Error> {
        Self::verify_inputs(blocks_limit, slot_from, slot_to, server_batch_size)?;

        let request_url = base_url
            .join(BLOCKS_STREAM.trim_start_matches('/'))
            .map_err(Error::Url)?;
        let mut request_url = request_url;
        {
            let mut query = request_url.query_pairs_mut();
            if let Some(blocks_limit) = blocks_limit {
                query.append_pair("blocks_limit", &blocks_limit.to_string());
            }
            if let Some(slot_from) = slot_from {
                query.append_pair("slot_from", &slot_from.to_string());
            }
            if let Some(slot_to) = slot_to {
                query.append_pair("slot_to", &slot_to.to_string());
            }
            if let Some(descending) = descending {
                query.append_pair("descending", &descending.to_string());
            }
            if let Some(server_batch_size) = server_batch_size {
                query.append_pair("server_batch_size", &server_batch_size.to_string());
            }
            if immutable_only == Some(true) {
                query.append_pair("immutable_only", "true");
            }
        }
        let mut request = self.client.get(request_url);

        if let Some(basic_auth) = &self.basic_auth {
            request = request.basic_auth(&basic_auth.username, basic_auth.password.as_deref());
        }

        let response = request.send().await.map_err(Error::Request)?;
        let status = response.status();
        let response_url = response.url().clone();

        if status != StatusCode::OK {
            let body = response.text().await.map_err(Error::Request)?;
            return match status {
                StatusCode::INTERNAL_SERVER_ERROR => {
                    Err(Error::Server(format!("{body} [{response_url}]")))
                }
                _ => Err(Error::Server(format!(
                    "Unexpected response [{status}] at [{response_url}]: {body}"
                ))),
            };
        }

        let blocks_stream = futures::stream::unfold(
            (
                response.bytes_stream(),
                Vec::<u8>::new(),
                VecDeque::<ProcessedBlockEvent>::new(),
            ),
            async |(mut byte_stream, mut buffer, mut pending)| {
                loop {
                    if let Some(event) = pending.pop_front() {
                        return Some((event, (byte_stream, buffer, pending)));
                    }

                    if let Some(Ok(bytes)) = byte_stream.next().await {
                        buffer.extend_from_slice(&bytes);

                        while let Some(newline_pos) = buffer.iter().position(|&b| b == b'\n') {
                            let line = buffer.drain(..=newline_pos).collect::<Vec<_>>();
                            let json_line = &line[..line.len().saturating_sub(1)];
                            if json_line.is_empty() {
                                continue;
                            }
                            if let Ok(event) =
                                serde_json::from_slice::<ProcessedBlockEvent>(json_line)
                            {
                                pending.push_back(event);
                            }
                        }
                    } else {
                        if !buffer.is_empty()
                            && let Ok(event) =
                                serde_json::from_slice::<ProcessedBlockEvent>(&buffer)
                        {
                            pending.push_back(event);
                        }

                        if let Some(event) = pending.pop_front() {
                            return Some((event, (byte_stream, Vec::new(), pending)));
                        }
                        return None;
                    }
                }
            },
        );
        Ok(blocks_stream)
    }

    /// Get the balance for a specific `ZkPublicKey`.
    pub async fn get_wallet_balance(
        &self,
        base_url: Url,
        zk_pk: ZkPublicKey,
        tip: Option<HeaderId>,
    ) -> Result<WalletBalanceResponseBody, Error> {
        let key_id = hex::encode(fr_to_bytes(zk_pk.as_fr()));
        let mut request_url = base_url
            .join(&BALANCE.replace(":public_key", &key_id))
            .map_err(Error::Url)?;

        if let Some(t) = tip {
            request_url
                .query_pairs_mut()
                .append_pair("tip", &t.to_string());
        }

        self.get::<(), WalletBalanceResponseBody>(request_url, None)
            .await
    }

    /// Post a request to transfer funds.
    pub async fn transfer_funds(
        &self,
        base_url: Url,
        body: WalletTransferFundsRequestBody,
    ) -> Result<WalletTransferFundsResponseBody, Error> {
        let request_url = base_url
            .join(TRANSACTIONS_TRANSFER_FUNDS.trim_start_matches('/'))
            .map_err(Error::Url)?;

        self.post(request_url, &body).await
    }
}
