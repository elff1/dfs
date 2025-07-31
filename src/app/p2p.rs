use std::{
    borrow::Cow,
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
    kad::{self, Mode, QueryId, QueryResult, Record, RecordKey, store::MemoryStore},
    mdns, multiaddr, noise, ping, relay,
    request_response::{self, OutboundRequestId, cbor},
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io, select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use self::{config::P2pServiceConfig, models::PublishedFile};
use super::{ServerError, Service};
use crate::{
    file_processor::{FileProcessResult, FileProcessResultHash, FileProcessor},
    file_store::{self, PublishedFileRecord},
};

pub mod config;
pub mod models;

const LOG_TARGET: &str = "app::p2p";

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileDownloadRequest {
    pub file_id: u64,
    pub chunk_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileDownloadResponse {
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetadataDownloadRequest {
    pub file_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetadataDownloadResponse {
    Success(Vec<u8>),
    Error(String),
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
    file_download: cbor::Behaviour<FileDownloadRequest, FileDownloadResponse>,
    metadata_download: cbor::Behaviour<MetadataDownloadRequest, MetadataDownloadResponse>,
}

pub enum P2pCommand {
    RequestMetadata {
        request: MetadataDownloadRequest,
        tx: oneshot::Sender<Option<FileProcessResult>>,
    },
}

#[derive(Debug)]
struct MeatadataDownloadRequestData {
    pub root_request: MetadataDownloadRequest,
    pub tx: Option<oneshot::Sender<Option<FileProcessResult>>>,
    pub kad_get_providers_query_id: Option<QueryId>,
    pub p2p_metadata_download_request_id: Option<OutboundRequestId>,
}

#[derive(Debug)]
pub struct P2pService<F: file_store::Store + Send + Sync + 'static> {
    config: P2pServiceConfig,
    file_store: F,
    file_publish_rx: mpsc::Receiver<FileProcessResult>,
    p2p_command_rx: mpsc::Receiver<P2pCommand>,
    metadata_download_requests: Vec<MeatadataDownloadRequestData>,
}

impl<F: file_store::Store + Send + Sync + 'static> P2pService<F> {
    pub fn new(
        config: P2pServiceConfig,
        file_store: F,
        file_publish_rx: mpsc::Receiver<FileProcessResult>,
        p2p_command_rx: mpsc::Receiver<P2pCommand>,
    ) -> Self {
        Self {
            config,
            file_store,
            file_publish_rx,
            p2p_command_rx,
            metadata_download_requests: vec![],
        }
    }

    async fn keypair(&self) -> Result<Keypair, P2pNetworkError> {
        if let Ok(data) = tokio::fs::read(&self.config.keypair_file).await {
            return Ok(Keypair::from_protobuf_encoding(&data)?);
        }

        let keypair = Keypair::generate_ed25519();
        let bin = keypair.to_protobuf_encoding()?;

        let dir =
            self.config
                .keypair_file
                .parent()
                .ok_or(P2pNetworkError::FailedToGetKeypairFileDir(
                    self.config.keypair_file.clone(),
                ))?;
        let _ = tokio::fs::remove_file(&self.config.keypair_file).await;
        tokio::fs::create_dir_all(dir).await?;
        tokio::fs::write(&self.config.keypair_file, bin).await?;

        Ok(keypair)
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
                    metadata_download: cbor::Behaviour::new(
                        [(
                            StreamProtocol::new("/dfs/1.0.0/metadata-download"),
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
    ) {
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
                        if let Err(e) = swarm.listen_on(listen_addr.clone()) {
                            log::warn!(target: LOG_TARGET, "Listen on relay with address {listen_addr} failed: {e:?}");
                        }
                    }
                }
            }
            _ => log::debug!(target: LOG_TARGET, "Identify event: {event:?}"),
        }
    }

    fn handle_mdns_event(&self, swarm: &mut Swarm<P2pNetworkBehaviour>, event: mdns::Event) {
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
    }

    fn handle_kademlia_event(
        &mut self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: kad::Event,
    ) -> Option<()> {
        match event {
            kad::Event::OutboundQueryProgressed {
                id: query_id,
                result,
                stats: _,
                step: _,
            } => {
                // let request_data_pos = self
                //     .metadata_download_requests
                //     .iter()
                //     .position(|d| d.kad_get_providers_query_id == query_id)?;
                // let request_data = self
                //     .metadata_download_requests
                //     .swap_remove(request_data_pos);
                let request_data = self
                    .metadata_download_requests
                    .iter_mut()
                    .find(|d| d.kad_get_providers_query_id == Some(query_id))?;

                let get_providers_result = match result {
                    QueryResult::GetProviders(result) => Some(result),
                    _ => None,
                }?
                .map_err(|e| {
                    log::error!(target: LOG_TARGET, "Get providers failed: {e:?}");
                })
                .ok()?;

                let neighbor_peer_id = match get_providers_result {
                    kad::GetProvidersOk::FoundProviders { key: _, providers } => {
                        providers.into_iter().next()
                    }
                    kad::GetProvidersOk::FinishedWithNoAdditionalRecord { mut closest_peers } => {
                        closest_peers.pop()
                    }
                }?;

                // avoid downloading metadata more than once
                request_data.kad_get_providers_query_id = None;

                let request_id = swarm
                    .behaviour_mut()
                    .metadata_download
                    .send_request(&neighbor_peer_id, request_data.root_request.clone());
                request_data.p2p_metadata_download_request_id = Some(request_id);
            }
            _ => {
                log::info!(target: LOG_TARGET, "Kademlia event: {event:?}")
            }
        }

        Some(())
    }

    fn handle_gossipsub_event(
        &self,
        _swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: gossipsub::Event,
    ) {
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
    }

    fn handle_file_download_event(
        &self,
        _swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: request_response::Event<FileDownloadRequest, FileDownloadResponse>,
    ) {
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
                        log::info!(target: LOG_TARGET, "File download request [{request:?}] from {peer}");
                        // let response = FileDownloadResponse { data: vec![] }; // Placeholder for actual data
                        // swarm
                        //     .behaviour_mut()
                        //     .file_download
                        //     .send_response(request, response)?;
                    }
                    request_response::Message::Response {
                        request_id: _,
                        response,
                    } => {
                        log::info!(target: LOG_TARGET, "File download response [{response:?}] from {peer}");
                    }
                }
            }
            _ => log::debug!(target: LOG_TARGET, "File download event: {event:?}"),
        }
    }

    async fn get_published_file_metadata(&self, file_id: u64) -> Result<Vec<u8>, Cow<str>> {
        let dir_path = self
            .file_store
            .get_published_file_chunks_directory(file_id)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Get DB record of file ID[{file_id}] failed: {e}");
                "File ID not found"
            })?;

        let metadata = FileProcessor::get_file_metadata(&dir_path).await.map_err(|e| {
            log::error!(target: LOG_TARGET, "Read metadata file[{}] of file ID[{file_id}] failed: {e}", dir_path.display());
            "File not exist"
        })?;

        Ok(metadata)
    }

    fn parse_metadata_download_response(
        &self,
        response: MetadataDownloadResponse,
    ) -> Option<FileProcessResult> {
        let metadata = match response {
            MetadataDownloadResponse::Success(metadata) => Some(metadata),
            MetadataDownloadResponse::Error(e) => {
                log::error!(target: LOG_TARGET, "Download metadata failed: {e}");
                None
            }
        }?;
        FileProcessResult::try_from(metadata)
            .map_err(|e| {
                log::error!(target: LOG_TARGET, "Parse metadata failed: {e}");
            })
            .ok()
    }

    async fn handle_metadata_download_event(
        &mut self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: request_response::Event<MetadataDownloadRequest, MetadataDownloadResponse>,
    ) -> Option<()> {
        match event {
            request_response::Event::Message {
                peer,
                connection_id: _,
                message,
            } => match message {
                request_response::Message::Request {
                    request_id: _,
                    request,
                    channel,
                } => {
                    log::info!(target: LOG_TARGET, "Metadata download request [{request:?}] from {peer}");
                    match self.get_published_file_metadata(request.file_id).await {
                        Ok(metadata) => {
                            swarm.behaviour_mut().metadata_download
                                    .send_response(channel, MetadataDownloadResponse::Success(metadata))
                                    .map_err(|_| {
                                        log::error!(target: LOG_TARGET, "Send metadata of file ID[{}] failed", request.file_id);
                                    }).ok()?;
                        }
                        Err(err_str) => {
                            swarm.behaviour_mut().metadata_download
                                    .send_response(channel, MetadataDownloadResponse::Error(err_str.into_owned()))
                                    .map_err(|_| {
                                        log::error!(target: LOG_TARGET, "Send error of download file ID[{}] metadata failed", request.file_id);
                                    }).ok()?;
                        }
                    };
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    log::info!(target: LOG_TARGET, "Metadata download response from {peer}");
                    let metadata = self.parse_metadata_download_response(response);

                    if let Some(ref metadata) = metadata {
                        log::info!(target: LOG_TARGET, "Metadata downloaded: [{}] with {} chunks",
                                    metadata.original_file_name, metadata.number_of_chunks);
                    }

                    self.metadata_download_requests
                        .iter_mut()
                        .find(|d| d.p2p_metadata_download_request_id == Some(request_id))?
                        .tx
                        .take()?
                        .send(metadata)
                        .map_err(|e| {
                            log::error!(target: LOG_TARGET, "Send downloaded metadata of [{:?}] back failed", e.map(|m| m.original_file_name));
                        }).ok()?;
                }
            },
            _ => log::debug!(target: LOG_TARGET, "Metadata download event: {event:?}"),
        }

        Some(())
    }

    fn handle_file_publish(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        processed_file: FileProcessResult,
    ) -> Option<()> {
        let file_store_record: PublishedFileRecord = (&processed_file).into();

        let kad_key: Vec<_> = file_store_record.key();
        log::info!(target: LOG_TARGET, "Publish processed file[{}] with {} chunks on DHT: {}/{}",
            processed_file.original_file_name, processed_file.number_of_chunks, file_store_record.id.raw() ,hex::encode(&kad_key));

        let kad_value = serde_cbor::to_vec(&PublishedFile::new(
            processed_file.number_of_chunks,
            processed_file.merkle_root,
        ))
        .map_err(|e| {
            log::error!(target: LOG_TARGET, "Cbor serialize file process result failed: {e:?}");
        })
        .ok()?;

        let kad_record = Record::new(kad_key, kad_value);
        let key = kad_record.key.clone();
        swarm
            .behaviour_mut()
            .kademlia
            .put_record(kad_record, kad::Quorum::Majority)
            .map_err(|e| log::error!(target: LOG_TARGET, "Put new record to DHT failed: {e:?}"))
            .ok();
        swarm
            .behaviour_mut()
            .kademlia
            .start_providing(key)
            .map_err(|e| log::error!(target: LOG_TARGET, "Provide new record to DHT failed: {e:?}"))
            .ok();

        self.file_store.add_published_file(file_store_record).unwrap_or_else(|e| {
            log::error!(target: LOG_TARGET, "Add new published file to file store failed: {e:?}")
        });

        Some(())
    }

    fn handle_command(&mut self, swarm: &mut Swarm<P2pNetworkBehaviour>, command: P2pCommand) {
        match command {
            P2pCommand::RequestMetadata { request, tx } => {
                let key = RecordKey::new(&FileProcessResultHash::new(request.file_id).to_array());
                let query_id = swarm.behaviour_mut().kademlia.get_providers(key);
                self.metadata_download_requests
                    .push(MeatadataDownloadRequestData {
                        root_request: request,
                        tx: Some(tx),
                        kad_get_providers_query_id: Some(query_id),
                        p2p_metadata_download_request_id: None,
                    });
            }
        }
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
                        P2pNetworkBehaviourEvent::Identify(event) => self.handle_identify_event(&mut swarm, event),
                        P2pNetworkBehaviourEvent::Mdns(event) => self.handle_mdns_event(&mut swarm, event),
                        P2pNetworkBehaviourEvent::Kademlia(event) => self.handle_kademlia_event(&mut swarm, event).unwrap_or_default(),
                        P2pNetworkBehaviourEvent::Gossipsub(event) => self.handle_gossipsub_event(&mut swarm, event),
                        P2pNetworkBehaviourEvent::RelayServer(event) => log::debug!(target: LOG_TARGET, "RelayServer event: {event:?}"),
                        P2pNetworkBehaviourEvent::RelayClient(event) => log::debug!(target: LOG_TARGET, "RelayClient event: {event:?}"),
                        P2pNetworkBehaviourEvent::Dcutr(event) => log::debug!(target: LOG_TARGET, "DCUTR event: {event:?}"),
                        P2pNetworkBehaviourEvent::FileDownload(event) => self.handle_file_download_event(&mut swarm, event),
                        P2pNetworkBehaviourEvent::MetadataDownload(event) => self.handle_metadata_download_event(&mut swarm, event).await.unwrap_or_default(),
                        _ => log::debug!(target: LOG_TARGET, "{event:?}"),
                    },
                    _ => log::debug!(target: LOG_TARGET, "Unhandled event: {event:?}"),
                },
                file_publish_reuslt = self.file_publish_rx.recv() => {
                    if let Some(result) = file_publish_reuslt {
                        self.handle_file_publish(&mut swarm, result);
                    }
                }
                command = self.p2p_command_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_command(&mut swarm, command);
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
