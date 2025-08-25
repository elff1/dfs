use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    ops::Not,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use libp2p::{
    StreamProtocol, Swarm, TransportError, dcutr,
    futures::stream::StreamExt,
    gossipsub::{self, IdentTopic, MessageAuthenticity, SubscriptionError},
    identify,
    identity::{DecodingError, Keypair},
    kad::{self, Mode, QueryId, QueryResult, Record, RecordKey, store::MemoryStore},
    mdns, multiaddr, noise, ping, relay,
    request_response::{self, OutboundRequestId, ResponseChannel, cbor},
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use thiserror::Error;
use tokio::{
    fs, io, select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use self::config::P2pServiceConfig;
use super::{
    ServerError, Service,
    fs::{FileProcessResult, FsChunkCommand, FsCommand, FsHelper},
};
use crate::file_store::{self, PublishedFileRecord};

pub mod config;
mod models;
pub use models::*;

const LOG_TARGET: &str = "app::p2p";
const MAX_PARALLEL_DOWNLOAD_CHUNK_NUM: usize = 16;

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

type FileDownloadRequest = FileChunkId;
type FsReadChannel = (
    mpsc::Sender<(FileChunkId, Option<Vec<u8>>)>,
    mpsc::Receiver<(FileChunkId, Option<Vec<u8>>)>,
);

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
}

// TODO: improvement:
// seperate DownloadChunk from P2pCommand,
// then P2pCommand contains PublishFile and DownloadMetadata
pub enum P2pCommand {
    PublishFile(Box<FileProcessResult>),
    DownloadFile {
        id: FileChunkId,
        downloaded_contents_tx: oneshot::Sender<Option<Vec<u8>>>,
    },
}

#[derive(Debug)]
struct FileDownloadLocalRequestBlock {
    chunk_id: FileChunkId,
    downloaded_contents_tx: Option<oneshot::Sender<Option<Vec<u8>>>>,
    start_timestamp: Instant,
    kad_get_providers_query_id: Option<QueryId>,
    p2p_file_download_request_id: Option<OutboundRequestId>,
}

#[derive(Debug)]
pub struct P2pService<F: file_store::Store + Send + Sync + 'static> {
    config: P2pServiceConfig,
    file_store: Arc<F>,

    // Communication with FS service
    fs_command_tx: mpsc::Sender<FsCommand>,
    fs_chunk_command_tx: async_channel::Sender<FsChunkCommand>,
    fs_read_channel: FsReadChannel,

    // Receive commads from other services
    p2p_command_rx: mpsc::Receiver<P2pCommand>,

    // inner states
    file_download_local_requests: VecDeque<Box<FileDownloadLocalRequestBlock>>,
    file_download_remote_requests: HashMap<FileChunkId, Vec<ResponseChannel<FileDownloadResponse>>>,
}

impl<F: file_store::Store + Send + Sync + 'static> P2pService<F> {
    pub fn new(
        config: P2pServiceConfig,
        file_store: Arc<F>,
        fs_command_tx: mpsc::Sender<FsCommand>,
        fs_chunk_command_tx: async_channel::Sender<FsChunkCommand>,
        p2p_command_rx: mpsc::Receiver<P2pCommand>,
    ) -> Self {
        Self {
            config,
            file_store,
            fs_command_tx,
            fs_chunk_command_tx,
            fs_read_channel: mpsc::channel(100),
            p2p_command_rx,
            file_download_local_requests: VecDeque::with_capacity(MAX_PARALLEL_DOWNLOAD_CHUNK_NUM),
            file_download_remote_requests: HashMap::new(),
        }
    }

    async fn keypair(&self) -> Result<Keypair, P2pNetworkError> {
        if let Ok(data) = fs::read(&self.config.keypair_file).await {
            return Ok(Keypair::from_protobuf_encoding(&data)?);
        }

        let keypair = Keypair::generate_ed25519();
        let contents = keypair.to_protobuf_encoding()?;

        let Some(dir) = self.config.keypair_file.parent() else {
            return Err(P2pNetworkError::FailedToGetKeypairFileDir(
                self.config.keypair_file.clone(),
            ));
        };
        let _ = fs::remove_file(&self.config.keypair_file).await;
        fs::create_dir_all(dir).await?;
        fs::write(&self.config.keypair_file, contents).await?;

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
                            log::warn!(target: LOG_TARGET, "Listen on relay with address {listen_addr} failed: {e}");
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
                // handle GetProviders result only
                let get_providers_result = match result {
                    QueryResult::GetProviders(result) => Some(result),
                    _ => None,
                }?;

                let request_block_pos = self.file_download_local_requests.iter().position(
                    |d| matches!(d.kad_get_providers_query_id, Some(id) if id == query_id),
                )?;
                let mut request_block = self
                    .file_download_local_requests
                    .remove(request_block_pos)?;

                let neighbor_peer_id = match get_providers_result {
                    Ok(kad::GetProvidersOk::FoundProviders { key: _, providers }) => {
                        providers.into_iter().next()
                    }
                    Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord {
                        mut closest_peers,
                    }) => closest_peers.pop(),
                    Err(e) => {
                        log::error!(target: LOG_TARGET, "Get providers of chunk[{}] failed: {e}", request_block.chunk_id);
                        return Some(());
                    }
                };

                let Some(neighbor_peer_id) = neighbor_peer_id else {
                    log::error!(target: LOG_TARGET, "No provider of chunk[{}] found", request_block.chunk_id);
                    return Some(());
                };

                let request_id = swarm
                    .behaviour_mut()
                    .file_download
                    .send_request(&neighbor_peer_id, request_block.chunk_id);
                request_block.p2p_file_download_request_id = Some(request_id);

                // Note: avoid downloading chunk more than once
                request_block.kad_get_providers_query_id = None;

                self.file_download_local_requests.push_back(request_block);
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

    async fn handle_file_download_event(
        &mut self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        event: request_response::Event<FileDownloadRequest, FileDownloadResponse>,
    ) -> Option<()> {
        match event {
            request_response::Event::Message {
                peer,
                connection_id: _,
                message,
            } => match message {
                request_response::Message::Request {
                    request_id: _,
                    request: chunk_id,
                    channel,
                } => {
                    let file_id = chunk_id.file_id;
                    let chunk_index = chunk_id.chunk_index;
                    log::info!(target: LOG_TARGET, "Download file[{chunk_id}] request from {peer}");

                    let entry = match self.file_download_remote_requests.entry(chunk_id) {
                        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().push(channel);
                            return Some(());
                        }
                        std::collections::hash_map::Entry::Vacant(vacant_entry) => vacant_entry,
                    };

                    let read_contents_tx = self.fs_read_channel.0.clone();
                    let fs_command_has_sent = match chunk_index {
                        0 => self
                            .fs_command_tx
                            .try_send(FsCommand::ReadMetadata {
                                file_id,
                                read_contents_tx,
                            })
                            .is_ok(),
                        _ => self
                            .fs_chunk_command_tx
                            .try_send(FsChunkCommand::Read {
                                chunk_id,
                                read_contents_tx,
                            })
                            .is_ok(),
                    };

                    if fs_command_has_sent {
                        entry.insert(vec![channel]);
                    } else {
                        let response = FileDownloadResponse::Error("busy".to_string());
                        swarm_file_download_response(swarm, channel, response, chunk_id);
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let request_block_pos = self.file_download_local_requests.iter().position(
                        |d| matches!(d.p2p_file_download_request_id, Some(id) if id == request_id),
                    )?;
                    let mut request_block = self
                        .file_download_local_requests
                        .remove(request_block_pos)?;

                    let contents = match response {
                        FileDownloadResponse::Success(contents) => Some(contents),
                        FileDownloadResponse::Error(e) => {
                            log::error!(target: LOG_TARGET, "Download chunk[{}] failed: {e}", request_block.chunk_id);
                            None
                        }
                    };

                    request_block.downloaded_contents_tx.take()?.send(contents)
                        .map_err(|_| {
                            log::error!(target: LOG_TARGET, "Send response of download chunk[{}] failed",
                                request_block.chunk_id)
                        }).ok()?;
                }
            },
            _ => log::debug!(target: LOG_TARGET, "File download event: {event:?}"),
        }

        Some(())
    }

    fn handle_file_read_result(
        &mut self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        chunk_id: FileChunkId,
        contents: Option<Vec<u8>>,
    ) -> Option<()> {
        let channels = self.file_download_remote_requests.remove(&chunk_id)
            .filter(|v| v.is_empty().not())
            .or_else(|| {
                log::warn!(target: LOG_TARGET, "File download remote request of chunk[{chunk_id}] not found");
                None
            })?;

        let response = match contents {
            Some(c) => FileDownloadResponse::Success(c),
            None => FileDownloadResponse::Error("internal error".to_string()),
        };

        let mut channels_num = channels.len();
        let mut last_channel = None;
        for channel in channels.into_iter() {
            if channels_num == 1 {
                last_channel = Some(channel);
                break;
            }

            channels_num -= 1;
            swarm_file_download_response(swarm, channel, response.clone(), chunk_id);
        }
        if let Some(channel) = last_channel {
            swarm_file_download_response(swarm, channel, response, chunk_id);
        }

        Some(())
    }

    fn handle_command_publish_file(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        processed_file: &FileProcessResult,
        file_store_record: PublishedFileRecord,
        need_store: bool,
    ) {
        let file_id = file_store_record.id.raw();
        log::info!(target: LOG_TARGET, "Publish processed file[{}][{file_id}] with {} chunks on DHT",
            file_store_record.original_file_name, processed_file.number_of_chunks);

        for chunk_index in 0..=(processed_file.number_of_chunks as usize) {
            let chunk_id = FileChunkId::new(file_id, chunk_index);
            let kad_key: Vec<u8> = (&chunk_id).into();
            let kad_record = Record::new(
                kad_key,
                match chunk_index {
                    0 => processed_file.merkle_root.to_vec(),
                    _ => processed_file.merkle_leaves[chunk_index].to_vec(),
                },
            );
            let kad_key = kad_record.key.clone();

            swarm
                .behaviour_mut()
                .kademlia
                .put_record(kad_record, kad::Quorum::Majority)
                .map_err(|e| log::error!(target: LOG_TARGET, "Put new record[{chunk_id}] to DHT failed: {e}"))
                .ok();
            swarm
                .behaviour_mut()
                .kademlia
                .start_providing(kad_key)
                .map_err(|e| log::error!(target: LOG_TARGET, "Provide new record[{chunk_id}] to DHT failed: {e}"))
                .ok();
        }

        if need_store {
            self.file_store.add_published_file(file_store_record).unwrap_or_else(|e| {
                log::error!(target: LOG_TARGET, "Add new published file to file store failed: {e}")
            });
        }
    }

    fn handle_command_download_file(
        &mut self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        chunk_id: FileChunkId,
        downloaded_contents_tx: oneshot::Sender<Option<Vec<u8>>>,
    ) -> Option<()> {
        let file_id = chunk_id.file_id;
        let chunk_index = chunk_id.chunk_index;
        log::info!(target: LOG_TARGET, "Start to download chunk[{chunk_id}]");

        // if file is downloaded or downloading, no need to download again
        if chunk_index == 0 {
            let mut file_exists = self.file_store.published_file_exists(file_id).map_err(|e| {
                    log::error!(target: LOG_TARGET, "Check whether file[{file_id}] is published failed: {e}");
                })
                .into_iter()
                .chain(self.file_store.downloading_file_exists(file_id).map_err(|e| {
                    log::error!(target: LOG_TARGET, "Check whether file[{file_id}] is downloading failed: {e}");
                }));

            let file_exists = match file_exists.next() {
                Some(false) => file_exists.next(),
                Some(true) => Some(true),
                None => None,
            };

            let Some(false) = file_exists else {
                if file_exists.is_some() {
                    log::warn!(target: LOG_TARGET, "No need to download file[{file_id}]");
                }

                return downloaded_contents_tx.send(None).map_err(|_| {
                    log::error!(target: LOG_TARGET, "Send response of download chunk[{}] failed", chunk_id)
                }).ok();
            };
        }

        let kad_key: Vec<u8> = (&chunk_id).into();
        let query_id = swarm
            .behaviour_mut()
            .kademlia
            .get_providers(RecordKey::new(&kad_key));

        self.file_download_local_requests
            .push_back(Box::new(FileDownloadLocalRequestBlock {
                chunk_id,
                downloaded_contents_tx: Some(downloaded_contents_tx),
                start_timestamp: Instant::now(),
                kad_get_providers_query_id: Some(query_id),
                p2p_file_download_request_id: None,
            }));

        Some(())
    }

    fn handle_command(&mut self, swarm: &mut Swarm<P2pNetworkBehaviour>, command: P2pCommand) {
        match command {
            P2pCommand::PublishFile(file_process_result) => {
                let file_store_record: PublishedFileRecord = file_process_result.as_ref().into();
                self.handle_command_publish_file(
                    swarm,
                    &file_process_result,
                    file_store_record,
                    true,
                );
            }
            P2pCommand::DownloadFile {
                id,
                downloaded_contents_tx,
            } => {
                self.handle_command_download_file(swarm, id, downloaded_contents_tx);
            }
        }
    }

    fn handle_tick(&mut self) {
        while let Some(request_block) = self.file_download_local_requests.pop_front() {
            let (timeout, timeout_stage) = if request_block.kad_get_providers_query_id.is_some() {
                (Duration::from_secs(20), "Get providers")
            } else {
                (Duration::from_secs(100), "Download contents")
            };

            if request_block.start_timestamp.elapsed() < timeout {
                self.file_download_local_requests.push_front(request_block);
                break;
            }

            log::error!(target: LOG_TARGET, "{} for chunk[{}] timeout", timeout_stage, request_block.chunk_id)
        }
    }

    async fn publish_one_stored_file(
        &self,
        swarm: &mut Swarm<P2pNetworkBehaviour>,
        file_store_record: PublishedFileRecord,
    ) -> Option<()> {
        let metadata: Box<FileProcessResult> =
            FsHelper::read_file_metadata_async(&file_store_record.chunks_directory)
                .await
                .map_err(|e| {
                    log::error!(target: LOG_TARGET, "Read metadata file[{}] ID[{}] failed: {e}",
                        file_store_record.chunks_directory.display(), file_store_record.id.raw());
                })
                .and_then(|metadata| {
                    metadata.as_slice().try_into().map_err(|e| {
                        log::error!(target: LOG_TARGET, "Parse metadata of file[{}] failed: {e}",
                            file_store_record.id.raw());
                    })
                })
                .ok()?;

        self.handle_command_publish_file(swarm, &metadata, file_store_record, false);

        Some(())
    }

    async fn publish_stored_files(&self, swarm: &mut Swarm<P2pNetworkBehaviour>) -> Option<()> {
        let records = self.file_store.get_all_published_files().map_err(|e| {
            log::error!(target: LOG_TARGET, "Get all published files from file store failed: {e}");
        }).ok()?;

        for file_store_record in records {
            self.publish_one_stored_file(swarm, file_store_record).await;
        }

        Some(())
    }

    async fn start_inner(mut self, cancel_token: CancellationToken) -> Result<(), P2pNetworkError> {
        let mut swarm = self.swarm().await?;
        log::info!(target: LOG_TARGET, "Peer ID: {}", swarm.local_peer_id());

        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));

        let file_owners_topic = IdentTopic::new("available_files");
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&file_owners_topic)?;

        self.publish_stored_files(&mut swarm).await;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

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
                            log::error!(target: LOG_TARGET, "Connection closed with {peer_id}: {err}");
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
                        P2pNetworkBehaviourEvent::FileDownload(event) => {
                            self.handle_file_download_event(&mut swarm, event).await.unwrap_or_default();
                        }
                        _ => log::debug!(target: LOG_TARGET, "{event:?}"),
                    },
                    _ => log::debug!(target: LOG_TARGET, "Unhandled event: {event:?}"),
                },
                file_read_result = self.fs_read_channel.1.recv() => {
                    if let Some((chunk_id, contents)) = file_read_result {
                        self.handle_file_read_result(&mut swarm, chunk_id, contents);
                    }
                },
                command = self.p2p_command_rx.recv(),
                    if self.file_download_local_requests.len() < MAX_PARALLEL_DOWNLOAD_CHUNK_NUM =>
                {
                    if let Some(command) = command {
                        self.handle_command(&mut swarm, command);
                    }
                },
                _ = interval.tick() => self.handle_tick(),
                _ = cancel_token.cancelled() => {
                    log::info!(target: LOG_TARGET, "P2P service is shutting down...");
                    break;
                },
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<F: file_store::Store + Send + Sync + 'static> Service for P2pService<F> {
    async fn start(self, cancel_token: CancellationToken) -> Result<(), ServerError> {
        log::info!(target: LOG_TARGET, "P2P service starting...");

        self.start_inner(cancel_token).await?;

        Ok(())
    }

    // async fn stop(&self) -> Result<(), Error> {
    //     // Logic to stop the P2P service
    //     Ok(())
    // }
}

impl Drop for FileDownloadLocalRequestBlock {
    fn drop(&mut self) {
        self.downloaded_contents_tx.take().map(|tx| {
            log::warn!(target: LOG_TARGET,
                "Download chunk[{}] timeout or failed, oneshot send None",
                self.chunk_id
            );

            // send empty content if download timeout or failed
            tx.send(None).map_err(|_| {
                log::error!(target: LOG_TARGET, "Send response of download chunk[{}] failed", self.chunk_id)
            })
        });
    }
}

fn swarm_file_download_response(
    swarm: &mut Swarm<P2pNetworkBehaviour>,
    channel: ResponseChannel<FileDownloadResponse>,
    response: FileDownloadResponse,
    chunk_id: FileChunkId,
) -> Option<()> {
    swarm
        .behaviour_mut()
        .file_download
        .send_response(channel, response)
        .map_err(|_| {
            log::error!(target: LOG_TARGET, "Send response of download file[{chunk_id}] failed");
        })
        .ok()
}
