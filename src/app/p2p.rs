use async_trait::async_trait;
use libp2p::{
    dcutr, gossipsub, identify,
    kad::{self, store::MemoryStore},
    mdns, ping, relay,
    request_response::cbor,
    swarm::NetworkBehaviour,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{Error, Service};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileChunkRequest {
    pub file_id: String,
    pub chunk_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileChunkReponse {
    pub data: Vec<u8>,
}

#[derive(NetworkBehaviour)]
pub struct P2pNetwork {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    mdns: mdns::Behaviour<mdns::tokio::Tokio>,
    kademlia: kad::Behaviour<MemoryStore>,
    gossipsub: gossipsub::Behaviour,
    relay_server: relay::Behaviour,
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    file_download: cbor::Behaviour<FileChunkRequest, FileChunkReponse>,
}

pub struct P2pService {}

impl P2pService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Service for P2pService {
    async fn start(&self, cancel_token: CancellationToken) -> Result<(), Error> {
        // Initialize and start the P2P network
        // let mut behaviour = P2pNetwork {
        //     ping: ping::Behaviour::new(ping::Config::default()),
        //     identify: identify::Behaviour::new(identify::Config::default()),
        //     mdns: mdns::Behaviour::new(mdns::Config::default()).await?,
        //     kademlia: kad::Behaviour::new(kad::Config::default(), MemoryStore::new()),
        //     gossipsub: gossipsub::Behaviour::new(gossipsub::ConfigBuilder::default().build()),
        //     relay_server: relay::Behaviour::new(relay::Config {
        //         client_mode: false,
        //         ..Default::default()
        //     }),
        //     relay_client: relay::client::Behaviour::new(relay::Config {
        //         client_mode: true,
        //         ..Default::default()
        //     }),
        //     dcutr: dcutr::Behaviour {},
        //     file_download: cbor::Behaviour::<FileChunkRequest, FileChunkReponse>::new(),
        // };

        // // Start the behaviour with the cancellation token
        // behaviour.start(cancel_token).await?;

        Ok(())
    }

    // async fn stop(&self) -> Result<(), Error> {
    //     // Logic to stop the P2P service
    //     Ok(())
    // }
}
