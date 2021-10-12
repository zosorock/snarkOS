// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

use crate::*;
use snarkos_metrics::{self as metrics, wrapped_mpsc};

use anyhow::*;
use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use rand::{thread_rng, Rng};
use snarkos_storage::DynStorage;
#[cfg(not(feature = "test"))]
use std::time::Duration;
use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    thread,
};
use tokio::{
    sync::{Mutex, RwLock},
    task,
    time::sleep,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum State {
    Idle = 0,
    Mining,
    Syncing,
}

#[derive(Default)]
pub struct StateCode(AtomicU8);

/// The internal state of a node.
pub struct InnerNode {
    /// The node's random numeric identifier.
    pub id: u64,
    /// The current state of the node.
    state: StateCode,
    /// The local address of this node.
    pub local_addr: OnceCell<SocketAddr>,
    /// The pre-configured parameters of this node.
    pub config: Config,
    /// The cache of node's inbound messages.
    pub inbound_cache: Mutex<BlockCache<{ crate::BLOCK_CACHE_SIZE }>>,
    /// The list of connected and disconnected peers of this node.
    pub peer_book: PeerBook,
    /// Tracks the known network crawled by this node.
    pub known_network: OnceCell<KnownNetwork>,
    /// The sync handler of this node.
    pub sync: OnceCell<Arc<Sync>>,
    /// The node's storage.
    pub storage: DynStorage,
    /// The node's start-up timestamp.
    pub launched: DateTime<Utc>,
    /// The tasks spawned by the node.
    tasks: DropJoin<task::JoinHandle<()>>,
    /// The threads spawned by the node.
    threads: DropJoin<thread::JoinHandle<()>>,
    /// An indicator of whether the node is shutting down.
    shutting_down: AtomicBool,
    pub(crate) master_dispatch: RwLock<Option<wrapped_mpsc::Sender<SyncInbound>>>,
    pub terminator: Arc<AtomicBool>,
}

/// A core data structure for operating the networking stack of this node.
#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Node(Arc<InnerNode>);

impl Deref for Node {
    type Target = Arc<InnerNode>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Node {
    /// Returns the current state of the node.
    #[inline]
    pub fn state(&self) -> State {
        match self.state.0.load(Ordering::SeqCst) {
            0 => State::Idle,
            1 => State::Mining,
            2 => State::Syncing,
            _ => unreachable!(),
        }
    }

    /// Changes the current state of the node.
    #[inline]
    pub fn set_state(&self, new_state: State) {
        let code = new_state as u8;

        self.state.0.store(code, Ordering::SeqCst);
    }
}

impl Node {
    /// Creates a new instance of `Node`.
    pub async fn new(config: Config, storage: DynStorage) -> Result<Self, NetworkError> {
        let node = Self(Arc::new(InnerNode {
            id: config.node_id.unwrap_or_else(|| thread_rng().gen()),
            state: Default::default(),
            local_addr: Default::default(),
            config,
            storage: storage.clone(),
            inbound_cache: Default::default(),
            peer_book: PeerBook::spawn(storage),
            sync: Default::default(),
            known_network: Default::default(),
            launched: Utc::now(),
            tasks: Default::default(),
            threads: Default::default(),
            shutting_down: Default::default(),
            master_dispatch: RwLock::new(None),
            terminator: Arc::new(AtomicBool::new(false)),
        }));

        if node.is_of_type(NodeType::Crawler) {
            // Safe since crawlers don't start the listener service.
            node.set_local_addr(node.config.desired_address);

            // Safe since this can only ever be set here.
            node.known_network.set(KnownNetwork::default()).unwrap();
        }

        Ok(node)
    }

    pub fn is_of_type(&self, t: NodeType) -> bool {
        self.config.node_type == t
    }

    pub fn set_sync(&mut self, sync: Sync) {
        if self.sync.set(Arc::new(sync)).is_err() {
            panic!("sync was set more than once!");
        }
    }

    /// Returns a reference to the sync objects.
    #[inline]
    pub fn sync(&self) -> Option<&Arc<Sync>> {
        self.sync.get()
    }

    /// Returns a reference to the sync objects, expecting them to be available.
    #[inline]
    pub fn expect_sync(&self) -> &Sync {
        self.sync().expect("no sync!")
    }

    #[inline]
    #[doc(hidden)]
    pub fn has_sync(&self) -> bool {
        self.sync().is_some()
    }

    pub fn known_network(&self) -> Option<&KnownNetwork> {
        self.known_network.get()
    }

    pub async fn start_services(&self) {
        let node_clone: Node = self.clone();
        let peer_sync_interval = self.config.peer_sync_interval();
        match self.storage.fetch_peers().await {
            Err(e) => {
                error!("failed to fetch peers from storage: {:?}", e);
            }
            Ok(peers) => {
                for peer in peers {
                    self.peer_book.add_peer(peer.address, Some(&peer)).await;
                }
            }
        }
        let peering_task = task::spawn(async move {
            loop {
                info!("Updating peers");

                node_clone.update_peers().await;

                sleep(peer_sync_interval).await;
            }
        });
        self.register_task(peering_task);

        if self.known_network().is_some() {
            let node_clone = self.clone();

            let known_network_task = task::spawn(async move {
                // Safe since we check the presence of `known_network`.
                let known_network = node_clone.known_network().unwrap();
                let mut receiver = known_network.take_receiver().unwrap();

                while let Some(message) = receiver.recv().await {
                    known_network.update(message);
                }
            });
            self.register_task(known_network_task);
        }

        if !self.is_of_type(NodeType::Crawler) {
            let node_clone = self.clone();
            let state_tracking_task = task::spawn(async move {
                loop {
                    sleep(std::time::Duration::from_secs(5)).await;

                    // Report node's current state.
                    trace!("Node state: {:?}", node_clone.state());
                }
            });
            self.register_task(state_tracking_task);
        }

        if self.sync().is_some() {
            let node_clone = self.clone();
            let block_sync_interval = node_clone.expect_sync().block_sync_interval();
            let sync_block_task = task::spawn(async move {
                loop {
                    if node_clone.peer_book.get_connected_peer_count() != 0 {
                        let is_syncing_blocks = node_clone.is_syncing_blocks();

                        if !is_syncing_blocks {
                            if let Err(e) = node_clone.run_sync().await {
                                error!("failed sync process: {:?}", e);
                            }
                            if let Err(e) = node_clone.expect_sync().consensus.fast_forward().await {
                                error!("failed to initiate fast forward for sync: {:?}", e);
                            };
                            node_clone.finished_syncing_blocks();
                        }
                    }

                    sleep(block_sync_interval).await;
                }
            });
            self.register_task(sync_block_task);

            // An arbitrary short delay before mempool sync attempts begin, to ensure that block sync
            // has been attempted first.
            #[cfg(not(feature = "test"))]
            sleep(Duration::from_secs(5)).await;

            // FIXME: sync providers are set as beacons in the configuration during the rollout.
            // let sync_providers = self.config.sync_providers();
            let sync_providers = self.config.beacons();
            let node_clone = self.clone();
            let mempool_sync_interval = node_clone.expect_sync().mempool_sync_interval();
            let sync_mempool_task = task::spawn(async move {
                loop {
                    if !node_clone.is_syncing_blocks() {
                        // Ensure that it's not just a short "break" from syncing blocks
                        #[cfg(not(feature = "test"))]
                        if let Some(elapsed) = node_clone.expect_sync().time_since_last_block_sync() {
                            if elapsed < Duration::from_secs(60) {
                                sleep(mempool_sync_interval).await;
                                continue;
                            }
                        }

                        // TODO (howardwu): Add some random sync nodes beyond this approach
                        //  to ensure some diversity in mempool state that is fetched.
                        //  For now, this is acceptable because we propogate the mempool to
                        //  all of our connected peers anyways.

                        // The order of preference for the sync node is as follows:
                        //   1. Iterate (in declared order) through the sync providers:
                        //      a. Check if this node is connected to the specified sync provider in the peer book.
                        //      b. Select the specified sync provider as the sync node if this node is connected to it.
                        //   2. If this node is not connected to any sync provider,
                        //      then select the last seen peer as the sync node.

                        // Step 1.
                        let mut sync_node = None;
                        for sync_provider in sync_providers.iter() {
                            if node_clone.peer_book.is_connected(*sync_provider) {
                                sync_node = Some(*sync_provider);
                                break;
                            }
                        }

                        // Step 2.
                        if sync_node.is_none() {
                            // Select last seen node as block sync node.
                            sync_node = node_clone.peer_book.last_seen().await;
                        }

                        node_clone.update_memory_pool(sync_node).await;
                    }

                    sleep(mempool_sync_interval).await;
                }
            });
            self.register_task(sync_mempool_task);
        }
    }

    pub async fn shut_down(&self) {
        debug!("Shutting down");

        for addr in self.connected_peers() {
            self.disconnect_from_peer(addr).await;
        }

        self.threads.flush();

        self.tasks.flush();
    }

    pub fn register_task(&self, handle: task::JoinHandle<()>) {
        self.tasks.append(handle);
    }

    pub fn register_thread(&self, handle: thread::JoinHandle<()>) {
        self.threads.append(handle);
    }

    /// Sets the local address of the node to the given value.
    #[inline]
    pub fn set_local_addr(&self, addr: SocketAddr) {
        self.local_addr
            .set(addr)
            .expect("local address was set more than once!");
    }

    #[inline]
    pub fn expect_local_addr(&self) -> SocketAddr {
        self.local_addr.get().copied().expect("no address set!")
    }

    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    pub async fn initialize_metrics(&self) -> Result<()> {
        debug!("Initializing metrics");
        if let Some(metrics_task) = snarkos_metrics::initialize() {
            self.register_task(metrics_task);
        }

        // The node can already be at some non-zero height.
        if self.sync().is_some() {
            metrics::gauge!(metrics::blocks::HEIGHT, self.storage.canon().await?.block_height as f64);
        }

        Ok(())
    }

    pub fn version(&self) -> Version {
        Version::new(crate::PROTOCOL_VERSION, self.expect_local_addr().port(), self.id)
    }

    pub async fn run_sync(&self) -> Result<()> {
        let (master, sender) = SyncAggro::new(self.clone());
        *self.master_dispatch.write().await = Some(sender);
        master.run().await
    }
}
