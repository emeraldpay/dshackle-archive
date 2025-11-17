use std::str::FromStr;
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
use crate::args;
use crate::errors::{BlockchainError};
use futures_util::stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use crate::archiver::range::Height;

pub struct Blockchain {
    parallel: Semaphore,
    dshackle: DshackleConn,
    dshackle_chain: i32,
}

#[derive(Clone)]
struct DshackleConn {
    emerald_conn: EmeraldConn,
}

impl Blockchain {
    pub async fn new(conn: &args::Connection, blockchain: i32) -> Result<Self, BlockchainError> {
        let dshackle = DshackleConn::new(conn).await?;
        Ok(Self {
            parallel: Semaphore::new(conn.parallel),
            dshackle,
            dshackle_chain: blockchain,
        })
    }

    pub async fn native_call(&self, method: &str, params: Vec<u8>) -> Result<Vec<u8>, BlockchainError> {
        let _permit = self.parallel.acquire().await.unwrap();
        let mut client = self.dshackle.client();
        let chain = self.dshackle_chain;

        let mut response = client
            .native_call(
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
            )
            .await?
            .into_inner();

        if let Some(resp) = response.next().await {
            match resp {
                Ok(value) => {
                    if value.succeed {
                        Ok(value.payload)
                    } else {
                        tracing::error!("Blockchain call failed. {}() -> {}", method, value.error_message);
                        Err(BlockchainError::FailResponse(method.to_string(), value.error_message))
                    }
                }
                Err(e) => {
                    tracing::warn!("Error response from blockchain. {}(). Status: {}", method, e);
                    Err(BlockchainError::IO)
                }
            }
        } else {
            tracing::warn!("No response from blockchain. {}()", method);
            Err(BlockchainError::IO)
        }
    }

    pub async fn subscribe_blocks(&self) -> Result<mpsc::Receiver<Height>, BlockchainError> {
        tracing::info!("Subscribe to blocks");
        let (tx, rx) = mpsc::channel(2);
        let mut client = self.dshackle.client();
        let chain = self.dshackle_chain;

        tokio::spawn(async move {
            let response =  client
                .subscribe_head(Chain {r#type: chain })
                .await
                .map_err(|e| {
                    tracing::error!("Cannot subscribe to head: {:?}", e);
                    BlockchainError::IO
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

    fn client(&self) -> BlockchainClient<AuthService<Channel>> {
        blockchain::connect(&self.emerald_conn)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(1024 * 1024 * 1024)
            .max_decoding_message_size(1024 * 1024 * 1024)
    }

}

pub type TransactionId = String;
