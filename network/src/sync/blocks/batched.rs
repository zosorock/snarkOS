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

use std::{collections::HashMap, net::SocketAddr};

use crate::{Node, Payload, Peer, SyncBase, SyncInbound};
use anyhow::*;
use hash_hasher::{HashBuildHasher, HashedMap, HashedSet};
use rand::{prelude::SliceRandom, rngs::SmallRng, SeedableRng};
use snarkos_metrics::wrapped_mpsc;
use snarkos_storage::Digest;
use snarkvm_algorithms::crh::double_sha256;
use snarkvm_dpc::BlockHeader;

/// Efficient but slow and fork-prone sync method that operates in distributed batches.
pub struct SyncBatched {
    base: SyncBase,
}

struct SyncBlock {
    address: SocketAddr,
    block: Vec<u8>,
    height: Option<u32>,
}

impl SyncBatched {
    pub fn new(node: Node) -> (Self, wrapped_mpsc::Sender<SyncInbound>) {
        let (base, sender) = SyncBase::new(node);
        let new = Self { base };
        (new, sender)
    }

    async fn send_sync_messages(&mut self, sync_nodes: Vec<Peer>) -> Result<usize> {
        info!("requested block information from {} peers", sync_nodes.len());
        let block_locator_hashes = SyncBase::block_locator_hashes(&self.base.node).await?;
        let mut future_set = vec![];

        for peer in sync_nodes.iter() {
            if let Some(handle) = self.base.node.peer_book.get_peer_handle(peer.address) {
                let locator_hashes = block_locator_hashes.clone();
                future_set.push(async move {
                    handle.send_payload(Payload::GetSync(locator_hashes), None).await;
                });
            }
        }
        let sent = future_set.len();
        futures::future::join_all(future_set).await;
        Ok(sent)
    }

    async fn receive_sync_hashes(&mut self, max_message_count: usize) -> HashMap<SocketAddr, Vec<Digest>> {
        const TIMEOUT: u64 = 5;
        let mut received_block_hashes = HashMap::new();

        self.base
            .receive_messages(TIMEOUT, TIMEOUT, |msg| {
                match msg {
                    SyncInbound::BlockHashes(addr, hashes) => {
                        received_block_hashes
                            .insert(addr, hashes.into_iter().map(|x| -> Digest { x.0.into() }).collect());
                    }
                    SyncInbound::Block(..) => {
                        warn!("received sync block prematurely");
                    }
                }
                //todo: fail if peer sends > 1 block hash packet
                received_block_hashes.len() >= max_message_count
            })
            .await;

        info!(
            "received {} hashes from {} peers in {} seconds",
            received_block_hashes
                .values()
                .map(|x: &Vec<Digest>| x.len())
                .sum::<usize>(),
            received_block_hashes.len(),
            TIMEOUT
        );

        received_block_hashes
    }

    async fn receive_sync_blocks(&mut self, block_count: usize) -> Vec<SyncBlock> {
        const TIMEOUT: u64 = 30;
        let mut blocks = vec![];

        self.base
            .receive_messages(TIMEOUT, 4, |msg| {
                match msg {
                    SyncInbound::BlockHashes(_, _) => {
                        // late, ignored
                    }
                    SyncInbound::Block(address, block, height) => {
                        blocks.push(SyncBlock { address, block, height });
                    }
                }
                blocks.len() >= block_count
            })
            .await;

        info!("received {} blocks in {} seconds", blocks.len(), TIMEOUT);

        blocks
    }

    fn order_block_hashes(input: &[(SocketAddr, Vec<Digest>)]) -> Vec<Digest> {
        let mut block_order = vec![];
        let mut seen = HashedSet::<&Digest>::with_hasher(HashBuildHasher::default());
        let mut block_index = 0;
        loop {
            let mut found_row = false;
            for (_, hashes) in input {
                if let Some(hash) = hashes.get(block_index) {
                    found_row = true;
                    if seen.contains(&hash) {
                        continue;
                    }
                    seen.insert(hash);
                    block_order.push(hash.clone());
                }
            }
            block_index += 1;
            if !found_row {
                break;
            }
        }
        block_order
    }

    fn block_peer_map(blocks: &[(SocketAddr, Vec<Digest>)]) -> HashedMap<Digest, Vec<SocketAddr>> {
        let mut block_peer_map = HashedMap::with_capacity_and_hasher(blocks.len(), HashBuildHasher::default());
        for (addr, hashes) in blocks {
            for hash in hashes {
                block_peer_map.entry(hash.clone()).or_insert_with(Vec::new).push(*addr);
            }
        }
        block_peer_map
    }

    #[allow(clippy::type_complexity)]
    fn get_peer_blocks(
        &mut self,
        blocks: &[Digest],
        block_peer_map: &HashedMap<Digest, Vec<SocketAddr>>,
    ) -> (
        Vec<SocketAddr>,
        HashedMap<Digest, SocketAddr>,
        HashMap<SocketAddr, Vec<Digest>>,
    ) {
        let mut peer_block_requests: HashMap<SocketAddr, Vec<Digest>> = HashMap::new();
        let mut block_peers = HashedMap::with_hasher(HashBuildHasher::default());
        for block in blocks {
            let peers = block_peer_map.get(block);
            if peers.is_none() {
                continue;
            }
            let random_peer = peers.unwrap().choose(&mut SmallRng::from_entropy());
            if random_peer.is_none() {
                continue;
            }
            block_peers.insert(block.clone(), *random_peer.unwrap());
            peer_block_requests
                .entry(*random_peer.unwrap())
                .or_insert_with(Vec::new)
                .push(block.clone());
        }
        let addresses: Vec<SocketAddr> = peer_block_requests.keys().copied().collect();
        (addresses, block_peers, peer_block_requests)
    }

    async fn request_blocks(&mut self, peer_block_requests: HashMap<SocketAddr, Vec<Digest>>) -> usize {
        let mut sent = 0usize;

        let mut future_set = vec![];
        for (addr, request) in peer_block_requests {
            if let Some(peer) = self.base.node.peer_book.get_peer_handle(addr) {
                sent += request.len();
                future_set.push(async move {
                    peer.expecting_sync_blocks(request.len() as u32).await;
                    peer.send_payload(Payload::GetBlocks(request), None).await;
                });
            }
        }
        futures::future::join_all(future_set).await;
        sent
    }

    pub async fn run(mut self) -> Result<()> {
        let sync_nodes = self.base.find_sync_nodes().await?;

        if sync_nodes.is_empty() {
            return Ok(());
        }

        self.base.node.register_block_sync_attempt();

        let hash_requests_sent = self.send_sync_messages(sync_nodes).await?;

        if hash_requests_sent == 0 {
            return Ok(());
        }

        let received_block_hashes = self.receive_sync_hashes(hash_requests_sent).await;

        if received_block_hashes.is_empty() {
            return Ok(());
        }

        let blocks = received_block_hashes.into_iter().collect::<Vec<_>>();

        let early_blocks = Self::order_block_hashes(&blocks[..]);
        let early_blocks_count = early_blocks.len();

        let early_block_states = self.base.node.storage.get_block_states(&early_blocks[..]).await?;
        let block_order: Vec<_> = early_blocks
            .into_iter()
            .zip(early_block_states.iter())
            .filter(|(_, status)| matches!(status, snarkos_storage::BlockStatus::Unknown))
            .map(|(hash, _)| hash)
            .collect();

        info!(
            "requesting {} blocks for sync, received headers for {} known blocks",
            block_order.len(),
            early_blocks_count - block_order.len()
        );
        if block_order.is_empty() {
            return Ok(());
        }

        let block_peer_map = Self::block_peer_map(&blocks[..]);

        let (peer_addresses, block_peers, peer_block_requests) =
            self.get_peer_blocks(&block_order[..], &block_peer_map);

        let sent_block_requests = self.request_blocks(peer_block_requests).await;

        let received_blocks = self.receive_sync_blocks(sent_block_requests).await;

        info!(
            "received {}/{} blocks for sync",
            received_blocks.len(),
            sent_block_requests
        );

        self.base.cancel_outstanding_syncs(&peer_addresses[..]).await;

        let mut blocks_by_hash: HashedMap<Digest, _> =
            HashedMap::with_capacity_and_hasher(received_blocks.len(), HashBuildHasher::default());

        for block in received_blocks {
            let block_header = &block.block[..BlockHeader::size()];
            let hash = double_sha256(block_header).into();
            blocks_by_hash.insert(hash, block);
        }

        for (i, hash) in block_order.iter().enumerate() {
            if let Some(block) = blocks_by_hash.remove(hash) {
                self.base
                    .node
                    .process_received_block(block.address, block.block, block.height, false)
                    .await?;
            } else {
                warn!(
                    "did not receive block {}/{} ({}...) from {} by sync deadline",
                    i,
                    block_order.len(),
                    &hash.to_string()[..8],
                    block_peers.get(hash).map(|x| x.to_string()).unwrap_or_default(),
                );
            }
        }

        Ok(())
    }
}
