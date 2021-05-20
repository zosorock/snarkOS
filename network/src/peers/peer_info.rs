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

use crate::DropJoin;
use atomic_instant::AtomicInstant;
use snarkos_storage::BlockHeight;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::task;

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum PeerStatus {
    Connected,
    Disconnected,
    NeverConnected,
}

#[derive(Debug, Default)]
pub struct PeerQuality {
    /// The current block height of this peer.
    pub block_height: AtomicU32,
    /// The timestamp of when the peer has been seen last.
    pub last_seen: AtomicU64,
    /// An indicator of whether a `Pong` message is currently expected from this peer.
    pub expecting_pong: AtomicBool,
    /// The timestamp of the last `Ping` sent to the peer.
    pub last_ping_sent: AtomicInstant,
    /// The time it took to send a `Ping` to the peer and for it to respond with a `Pong`.
    pub rtt_ms: AtomicU64,
    /// The number of failures associated with the peer; grounds for dismissal.
    pub failures: AtomicU32,
    /// The number of remaining blocks to sync with.
    pub remaining_sync_blocks: AtomicU32,
    /// The number of messages received from the peer.
    pub num_messages_received: AtomicU64,
}

impl PeerQuality {
    pub fn is_inactive(&self, now: DateTime<Utc>) -> bool {
        let last_seen = self.last_seen();
        if let Some(last_seen) = last_seen {
            now - last_seen > chrono::Duration::seconds(crate::MAX_PEER_INACTIVITY_SECS.into())
        } else {
            // Impossible to trigger, as completing a connection
            // marks the peer with a timestamp. That being said,
            // it's safest to leave this as `true` for future-proofing.
            true
        }
    }

    pub fn see(&self) {
        self.last_seen
            .store(chrono::Utc::now().timestamp_millis() as u64, Ordering::SeqCst);
    }

    pub fn last_seen(&self) -> Option<DateTime<Utc>> {
        let now = self.last_seen.load(Ordering::SeqCst);
        if now == 0 {
            None
        } else {
            Some(DateTime::from_utc(
                NaiveDateTime::from_timestamp((now / 1000) as i64, (now % 1000) as u32),
                Utc,
            ))
        }
    }
}

/// A data structure containing information about a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// The IP address of this peer.
    address: SocketAddr,
    /// The timestamp of the first seen instance of this peer.
    first_seen: Option<DateTime<Utc>>,
    /// The timestamp of the last seen instance of this peer.
    last_connected: Option<DateTime<Utc>>,
    /// The timestamp of the last disconnect from this peer.
    last_disconnected: Option<DateTime<Utc>>,
    /// The number of times we have connected to this peer.
    connected_count: u64,
    /// The quality of the connection with the peer.
    #[serde(skip)]
    pub quality: Arc<PeerQuality>,
    /// The handles for tasks associated exclusively with this peer;
    /// The bool indicates whether it's abortable - otherwise it
    /// needs to be awaited instead.
    #[serde(skip)]
    tasks: DropJoin<task::JoinHandle<()>>,
}

impl PeerInfo {
    ///
    /// Creates a new instance of `PeerInfo`.
    ///
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            first_seen: None,
            last_connected: None,
            last_disconnected: None,
            connected_count: 0,
            quality: Default::default(),
            tasks: Default::default(),
        }
    }

    ///
    /// Returns the IP address of this peer.
    ///
    #[inline]
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    ///
    /// Returns the current block height of this peer.
    ///
    #[inline]
    pub fn block_height(&self) -> BlockHeight {
        self.quality.block_height.load(Ordering::SeqCst)
    }

    ///
    /// Returns the timestamp of the last seen instance of this peer.
    ///
    #[inline]
    pub fn last_seen(&self) -> Option<DateTime<Utc>> {
        let now = self.quality.last_seen.load(Ordering::SeqCst);
        if now == 0 {
            None
        } else {
            Some(DateTime::from_utc(
                NaiveDateTime::from_timestamp((now / 1000) as i64, (now % 1000) as u32),
                Utc,
            ))
        }
    }

    ///
    /// Returns the timestamp of the last connection to this peer.
    ///
    #[inline]
    pub fn last_connected(&self) -> Option<DateTime<Utc>> {
        self.last_connected
    }

    ///
    /// Returns the timestamp of the last disconnect from this peer.
    ///
    #[inline]
    pub fn last_disconnected(&self) -> Option<DateTime<Utc>> {
        self.last_disconnected
    }

    ///
    /// Returns the number of times we have connected to this peer.
    ///
    #[inline]
    pub fn connected_count(&self) -> u64 {
        self.connected_count
    }

    ///
    /// Updates the peer to connected.
    ///
    pub(crate) fn set_connected(&mut self) {
        let now = Utc::now();
        if self.first_seen.is_none() {
            self.first_seen = Some(now);
        }
        self.last_connected = Some(now);
        self.quality.see();
        self.connected_count += 1;
    }

    ///
    /// Updates the peer to disconnected.
    ///
    /// If the peer is not transitioning from `PeerStatus::Connecting` or `PeerStatus::Connected`,
    /// this function returns a `NetworkError`.
    ///
    pub(crate) fn set_disconnected(&mut self) {
        self.last_disconnected = Some(Utc::now());
        self.quality.expecting_pong.store(false, Ordering::SeqCst);
        self.quality.remaining_sync_blocks.store(0, Ordering::SeqCst);

        self.tasks.flush();
    }

    pub(crate) fn register_task(&self, handle: task::JoinHandle<()>) {
        self.tasks.append(handle);
    }
}
