use std::{
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use libp2p::{
    StreamProtocol, Swarm, TransportError, dcutr,
    futures::StreamExt,
    gossipsub::{self, IdentTopic, MessageAuthenticity, SubscriptionError},
    identify,
    identity::{DecodingError, Keypair},
    kad::{self, Mode, Record, store::MemoryStore},
    mdns, multiaddr, noise, ping, relay,
    request_response::{self, cbor},
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use rs_sha256::Sha256Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{io, select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    app::{
        ServerError, Service,
        p2p::{config::P2pServiceConfig, models::PublishedFile},
    },
    file_processor::FileProcessResult,
    file_store,
};

const LOG_TARGET: &str = "app::p2p::service";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileChunkRequest {
    pub file_id: String,
    pub chunk_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileChunkResponse {
    pub data: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum P2pNetworkError {
    #[error("Failed to get directory of the keypair file: {0}")]
    FailedToGetKeypairFileDir(PathBuf),
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    #[error("Keypair decoding error: {0}")]
    KeypairDecoding(#[from] DecodingError),
    #[error("Libp2p noise error: {0}")]
    Libp2pNoise(#[from] libp2p::noise::Error),
    #[error("Libp2p swarm builder error: {0}")]
    Libp2pSwarmBuilder(String),
    #[error("Parse P2p multiaddr error: {0}")]
    Libp2pMultiAddrParse(#[from] multiaddr::Error),
    #[error("Libp2p transport error: {0}")]
    Libp2pTransport(#[from] TransportError<io::Error>),
    #[error("Libp2p gossipsub subscription error: {0}")]
    Libp2pGossipsubSubscription(#[from] SubscriptionError),
}

#[derive(NetworkBehaviour)]
pub struct P2pNetworkBehaviour {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    mdns: mdns::Behaviour<mdns::tokio::Tokio>,
    kademlia: kad::Behaviour<MemoryStore>,
    gossipsub: gossipsub::Behaviour,
    relay_server: relay::Behaviour,
    relay_client: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    file_download: cbor::Behaviour<FileChunkRequest, FileChunkResponse>,
}

#[derive(Debug)]
pub struct P2pService<F: file_store::Store + Send + Sync + 'static> {
    config: P2pServiceConfig,
    file_publish_rx: mpsc::Receiver<FileProcessResult>,
    file_store: F,
}

impl<F: file_store::Store + Send + Sync + 'static> P2pService<F> {
    pub fn new(
        config: P2pServiceConfig,
        file_publish_rx: mpsc::Receiver<FileProcessResult>,
        file_store: F,
    ) -> Self {
        Self {
            config,
            file_publish_rx,
            file_store,
        }
    }

    async fn keypair(&self) -> Result<Keypair, P2pNetworkError> {
        match tokio::fs::read(&self.config.keypair_file).await {
            Ok(data) => Ok(Keypair::from_protobuf_encoding(&data)?),
            Err(_) => {
                let keypair = Keypair::generate_ed25519();
                let bin = keypair.to_protobuf_encoding()?;

                let dir = self.config.keypair_file.parent().ok_or(
                    P2pNetworkError::FailedToGetKeypairFileDir(self.config.keypair_file.clone()),
                )?;
                let _ = tokio::fs::remove_file(&self.config.keypair_file).await;
                tokio::fs::create_dir_all(dir).await?;
                tokio::fs::write(&self.config.keypair_file, bin).await?;

                Ok(keypair)
            }
        }
    }

    async fn swarm(&self) -> Result<Swarm<P2pNetworkBehaviour>, P2pNetworkError> {
        let keypair = self.keypair().await?;
        let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|key_pair, relay_client| {
                let mut kad_config = kad::Config::new(StreamProtocol::new("/dfs/1.0.0/kad"));
                kad_config.set_periodic_bootstrap_interval(Some(Duration::from_secs(30)));

                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .message_id_fn(|message| {
                        let mut hasher = DefaultHasher::new();
                        message.data.hash(&mut hasher);
                        message.topic.hash(&mut hasher);
                        if let Some(peer_id) = message.source {
                            peer_id.hash(&mut hasher);
                        }
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        now.to_string().hash(&mut hasher);
                        gossipsub::MessageId::from(hasher.finish().to_string())
                    })
                    .build()?;

                Ok(P2pNetworkBehaviour {
                    ping: ping::Behaviour::new(
                        ping::Config::new().with_interval(Duration::from_secs(20)),
                    ),
                    identify: identify::Behaviour::new(identify::Config::new(
                        "/dfs/1.0.0".to_string(),
                        key_pair.public(),
                    )),
                    mdns: mdns::Behaviour::new(
                        mdns::Config::default(),
                        key_pair.public().to_peer_id(),
                    )?,
                    kademlia: kad::Behaviour::with_config(
                        key_pair.public().to_peer_id(),
                        MemoryStore::new(key_pair.public().to_peer_id()),
                        kad_config,
                    ),
                    gossipsub: gossipsub::Behaviour::new(
                        MessageAuthenticity::Signed(key_pair.clone()),
                        gossipsub_config,
                    )?,
                    relay_server: relay::Behaviour::new(
                        key_pair.public().to_peer_id(),
                        relay::Config::default(),
                    ),
                    relay_client,
                    dcutr: dcutr::Behaviour::new(key_pair.public().to_peer_id()),
                    file_download: cbor::Behaviour::new(
                        [(
                            StreamProtocol::new("/dfs/1.0.0/file-download"),
                            request_response::ProtocolSupport::Full,
                        )],
                        request_response::Config::default(),
                    ),
                })
            })
            .map_err(|e| P2pNetworkError::Libp2pSwarmBuilder(e.to_string()))?
            .with_swarm_config(|config| {
                config.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
            })
            .build();

        Ok(swarm)
    }

    fn handle_identify_event(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: identify::Event,
    ) -> Result<(), P2pNetworkError> {
        match event {
            identify::Event::Received {
                connection_id: _,
                peer_id,
                info,
            } => {
                let is_relay = info.protocols.contains(&relay::HOP_PROTOCOL_NAME);

                for addr in info.listen_addrs {
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr.clone());
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);

                    if is_relay {
                        let listen_addr = addr
                            .with_p2p(peer_id)
                            .unwrap()
                            .with(multiaddr::Protocol::P2pCircuit);
                        log::info!(target: LOG_TARGET, "Try listen on relay with address {listen_addr}");
                        swarm.listen_on(listen_addr)?;
                    }
                }
            }
            _ => log::debug!(target: LOG_TARGET, "Identify event: {event:?}"),
        }
        Ok(())
    }

    fn handle_mdns_event(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: mdns::Event,
    ) -> Result<(), P2pNetworkError> {
        match event {
            mdns::Event::Discovered(peers) => {
                for (peer_id, addr) in peers {
                    log::info!(target: LOG_TARGET, "[mDNS] Discovered peer {peer_id} at {addr}");
                    swarm.add_peer_address(peer_id, addr.clone());
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }
            _ => log::debug!(target: LOG_TARGET, "mDNS event: {event:?}"),
        }
        Ok(())
    }

    fn handle_gossipsub_event(
        &self,
        _swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: gossipsub::Event,
    ) -> Result<(), P2pNetworkError> {
        match event {
            gossipsub::Event::Message {
                propagation_source: _,
                message_id: _,
                message,
            } => {
                log::info!(target: LOG_TARGET, "[Gossipsub] New message: {message:?}");
            }
            _ => log::debug!(target: LOG_TARGET, "Gossipsub event: {event:?}"),
        }

        Ok(())
    }

    fn handle_file_download_event(
        &self,
        _swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: request_response::Event<FileChunkRequest, FileChunkResponse>,
    ) -> Result<(), P2pNetworkError> {
        match event {
            request_response::Event::Message {
                peer,
                connection_id: _,
                message,
            } => {
                match message {
                    request_response::Message::Request {
                        request_id: _,
                        request,
                        channel: _,
                    } => {
                        log::info!(target: LOG_TARGET, "File download request {request:?} from {peer}");
                        // let response = FileChunkResponse { data: vec![] }; // Placeholder for actual data
                        // swarm
                        //     .behaviour_mut()
                        //     .file_download
                        //     .send_response(request, response)?;
                    }
                    request_response::Message::Response {
                        request_id: _,
                        response,
                    } => {
                        log::info!(target: LOG_TARGET, "File download response {response:?} from {peer}");
                    }
                }
            }
            _ => log::debug!(target: LOG_TARGET, "Message event: {event:?}"),
        }

        Ok(())
    }

    fn handle_file_publish(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        processed_file: FileProcessResult,
    ) -> Result<(), P2pNetworkError> {
        log::info!(target: LOG_TARGET, "Publish processed file[{}] with {} chunks", processed_file.original_file_name, processed_file.number_of_chunks);

        let mut hasher = Sha256Hasher::default();
        processed_file.hash(&mut hasher);
        let raw_key = hasher.finish().to_be_bytes().to_vec();
        log::info!(target: LOG_TARGET, "New file[{}] on DHT: {}", processed_file.original_file_name, hex::encode(&raw_key));

        match serde_cbor::to_vec(&PublishedFile::new(
            processed_file.number_of_chunks,
            processed_file.merkle_root,
        )) {
            Ok(bin) => {
                let record = Record::new(raw_key, bin);
                let key = record.key.clone();
                if let Err(e) = swarm
                    .behaviour_mut()
                    .kademlia
                    .put_record(record, kad::Quorum::Majority)
                {
                    log::error!(target: LOG_TARGET, "Failed to put new record to DHT: {e:?}")
                }
                if let Err(e) = swarm.behaviour_mut().kademlia.start_providing(key) {
                    log::error!(target: LOG_TARGET, "Failed to provide new record to DHT: {e:?}")
                }

                if let Err(e) = self.file_store.add_published_file(processed_file.into()) {
                    log::error!(target: LOG_TARGET, "Failed to add new published file to file store: {e:?}")
                }
            }
            Err(e) => {
                log::error!(target: LOG_TARGET, "Failed to cbor serialize file process result: {e:?}")
            }
        }

        Ok(())
    }

    async fn start_inner(mut self, cancel_token: CancellationToken) -> Result<(), P2pNetworkError> {
        let mut swarm = self.swarm().await?;
        log::info!(target: LOG_TARGET, "Peer ID: {:?}", swarm.local_peer_id());

        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));

        let file_owners_topic = IdentTopic::new("available_files");
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&file_owners_topic)?;

        loop {
            select! {
                event = swarm.select_next_some() => match event {
                    libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                        log::info!(target: LOG_TARGET, "Listening on: {address}");
                    },
                    libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        log::info!(target: LOG_TARGET, "Connection established with: {peer_id}");
                    },
                    libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                        if let Some(err) = cause {
                            log::error!(target: LOG_TARGET, "Connection closed with {peer_id}: {err:?}");
                        } else {
                            log::info!(target: LOG_TARGET, "Connection closed with: {peer_id}");
                        }
                    },
                    libp2p::swarm::SwarmEvent::Behaviour(event) => match event {
                        P2pNetworkBehaviourEvent::Identify(event) => self.handle_identify_event(&mut swarm, event)?,
                        P2pNetworkBehaviourEvent::Mdns(event) => self.handle_mdns_event(&mut swarm, event)?,
                        P2pNetworkBehaviourEvent::Kademlia(event) => log::info!(target: LOG_TARGET, "Kademlia event: {event:?}"),
                        P2pNetworkBehaviourEvent::Gossipsub(event) => self.handle_gossipsub_event(&mut swarm, event)?,
                        P2pNetworkBehaviourEvent::RelayServer(event) => log::debug!(target: LOG_TARGET, "RelayServer event: {event:?}"),
                        P2pNetworkBehaviourEvent::RelayClient(event) => log::debug!(target: LOG_TARGET, "RelayClient event: {event:?}"),
                        P2pNetworkBehaviourEvent::Dcutr(event) => log::debug!(target: LOG_TARGET, "DCUTR event: {event:?}"),
                        P2pNetworkBehaviourEvent::FileDownload(event) => self.handle_file_download_event(&mut swarm, event)?,
                        _ => log::debug!(target: LOG_TARGET, "{event:?}"),
                    },
                    _ => log::debug!(target: LOG_TARGET, "Unhandled event: {event:?}"),
                },
                file_publish_reuslt = self.file_publish_rx.recv() => {
                    if let Some(result) = file_publish_reuslt {
                        self.handle_file_publish(&mut swarm, result)?;
                    }
                }
                _ = cancel_token.cancelled() => {
                    log::info!(target: LOG_TARGET, "P2P service is shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<F: file_store::Store + Send + Sync + 'static> Service for P2pService<F> {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        log::debug!(target: LOG_TARGET, "P2pService starting...");

        self.start_inner(cancel_token).await?;

        Ok(())
    }

    // async fn stop(&self) -> Result<(), Error> {
    //     // Logic to stop the P2P service
    //     Ok(())
    // }
}
