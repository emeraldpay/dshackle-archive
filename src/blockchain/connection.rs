use std::str::FromStr;
use std::time::Duration;
use emerald_api::{
    blockchain,
    proto::{
        common::Chain,
        blockchain::{
            blockchain_client::BlockchainClient,
            NativeCallItem,
            NativeCallRequest
        }
    },
    conn::EmeraldConn,
    creds::{AuthService, Credentials}
};
use ginepro::{LoadBalancedChannelBuilder};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use crate::args;
use crate::errors::{BlockchainError};
use futures_util::stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use crate::archiver::range::Height;

const USER_AGENT: &str = concat!("EmeraldDshackleArchive/", env!("CARGO_PKG_VERSION"));

pub struct Blockchain {
    parallel: Semaphore,
    dshackle: DshackleConn,
    dshackle_chain: i32,
    blockchain_id: String,
}

#[derive(Clone)]
struct DshackleConn {
    emerald_conn: EmeraldConn,
}

impl Blockchain {
    pub async fn new(conn: &args::Connection, blockchain: i32, blockchain_id: String) -> Result<Self, BlockchainError> {
        let dshackle = DshackleConn::new(conn).await?;
        let threads = crate::global::get_threads();
        Ok(Self {
            parallel: Semaphore::new(threads.api),
            dshackle,
            dshackle_chain: blockchain,
            blockchain_id,
        })
    }

    ///
    /// Execute nativeCall with the configured API timeout (see [`crate::global::TimeoutsConfig`]).
    pub async fn native_call(&self, method: &str, params: Vec<u8>) -> Result<Vec<u8>, BlockchainError> {
        self.native_call_with_timeout(method, params, crate::global::get_timeouts().api).await
    }

    ///
    /// Execute nativeCall, failing with [`BlockchainError::Timeout`] if the response doesn't come within `limit`.
    /// The time spent waiting for a free parallel slot doesn't count towards the limit.
    pub async fn native_call_with_timeout(&self, method: &str, params: Vec<u8>, limit: Duration) -> Result<Vec<u8>, BlockchainError> {
        let _permit = self.parallel.acquire().await.unwrap();
        let chain = self.dshackle_chain;
        let start = std::time::Instant::now();

        let result = timeout(limit,
                             Self::native_call_inner(self.dshackle.clone(), chain, method, params)
        ).await
            .map_err(|_| BlockchainError::Timeout(method.to_string()))?
            .map_err(|e| {
                // Only this attempt; the retrying caller reports the fetch if it doesn't recover.
                tracing::debug!("Error calling blockchain method {}: {}", method, e);
                e
            });

        crate::metrics::observe_request(method, &self.blockchain_id, start.elapsed().as_secs_f64());
        result
    }

    async fn native_call_inner(dshackle: DshackleConn, chain: i32, method: &str, params: Vec<u8>) -> Result<Vec<u8>, BlockchainError> {
        let mut client = dshackle.client();
        let mut response = client
            .native_call(DshackleConn::request(
                NativeCallRequest {
                    chain,
                    items: vec![
                        NativeCallItem {
                            id: 1,
                            method: method.to_string(),
                            payload: params,
                            ..NativeCallItem::default()
                        }
                    ],
                    ..NativeCallRequest::default()
                }
            ))
            .await
            .map_err(|e| BlockchainError::IO(method.to_string(), e.to_string()))?
            .into_inner();

        let result = if let Some(resp) = response.next().await {
            match resp {
                Ok(value) => {
                    if value.succeed {
                        Ok(value.payload)
                    } else {
                        Err(BlockchainError::FailResponse(method.to_string(), value.error_message))
                    }
                }
                Err(e) => Err(BlockchainError::IO(method.to_string(), e.to_string())),
            }
        } else {
            Err(BlockchainError::IO(method.to_string(), "no response".to_string()))
        };

        // Read the stream up to its end, even though the request has only one item. Dropping it before the trailers
        // arrive makes h2 cancel the stream with RST_STREAM, and the trailers that come later for the forgotten stream
        // count as protocol errors. After 1024 of them h2 closes the whole connection with GOAWAY (too_many_internal_resets),
        // failing all requests in flight on it.
        while let Some(extra) = response.next().await {
            if let Err(e) = extra {
                tracing::debug!("Error at the end of the response. {}(). Status: {}", method, e);
            }
        }
        result
    }

    pub async fn subscribe_blocks(&self) -> Result<mpsc::Receiver<Height>, BlockchainError> {
        tracing::info!("Subscribe to blocks");
        let (tx, rx) = mpsc::channel(2);
        let mut client = self.dshackle.client();
        let chain = self.dshackle_chain;

        tokio::spawn(async move {
            let response =  client
                .subscribe_head(DshackleConn::request(Chain {r#type: chain }))
                .await
                .map_err(|e| {
                    tracing::error!("Cannot subscribe to head: {:?}", e);
                    BlockchainError::IO("subscribeHead".to_string(), e.to_string())
                });

            if let Err(e) = response {
                tracing::error!("Cannot request head: {:?}", e);
                return;
            }

            let mut responses =  response.unwrap().into_inner();

            while let Some(resp) = responses.next().await {
                match resp {
                    Ok(head) => {
                        tracing::info!("At Height: {}", head.height);
                        let n = Height {
                            height: head.height,
                            hash: Some(head.block_id),
                        };
                        let _ = tx.send(n).await;
                    },
                    Err(e) => {
                        tracing::warn!("Connection error for block subscription: {}", e)
                        //TODO reconnect
                    }
                }
            }
        });

        Ok(rx)
    }
}

impl DshackleConn {
    async fn new(config: &args::Connection) -> Result<DshackleConn, BlockchainError> {
        let url = DshackleConn::get_service(&config)
            .map_err(|_| BlockchainError::NoConnection)?;

        let channel = LoadBalancedChannelBuilder::new_with_service(url)
            .dns_probe_interval(std::time::Duration::from_secs(10))
            .channel()
            .await
            .map_err(|_| BlockchainError::NoConnection)?;

        let emerald_conn = EmeraldConn::new(Channel::from(channel), Credentials::unauthenticated());

        Ok(DshackleConn {
            emerald_conn,
        })
    }

    fn get_service(config: &args::Connection) -> Result<(String, u16), BlockchainError> {
        let url = config.connection.trim_start_matches("http://");
        let parts = url.split_once(":").ok_or(BlockchainError::InvalidConnection(url.to_string()))?;
        let port = u16::from_str(parts.1).map_err(|_| BlockchainError::InvalidConnection(url.to_string()))?;
        Ok((parts.0.to_string(), port))
    }

    /// Ginepro builds the tonic endpoints itself, without a way to set their user agent, so it goes with each request instead.
    /// Tonic keeps it and appends its own `tonic/x.y` after it.
    fn request<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.metadata_mut().insert("user-agent", MetadataValue::from_static(USER_AGENT));
        request
    }

    fn client(&self) -> BlockchainClient<AuthService<Channel>> {
        blockchain::connect(&self.emerald_conn)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(1024 * 1024 * 1024)
            .max_decoding_message_size(1024 * 1024 * 1024)
    }

}

pub type TransactionId = String;
