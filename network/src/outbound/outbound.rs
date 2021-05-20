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

use crate::{stats, ConnWriter, Direction, Message, NetworkError, Node, Payload};

use tokio::sync::mpsc::{self, error::TrySendError};

use mpmc_map::MpmcMap;
use snarkvm_objects::Storage;

use std::net::SocketAddr;

/// A core data structure for handling outbound network traffic.
#[derive(Debug, Default)]
pub struct Outbound {
    /// The map of remote addresses to their active write channels.
    pub(crate) channels: MpmcMap<SocketAddr, mpsc::Sender<Message>>,
}

impl<S: Storage + Send + Sync + 'static> Node<S> {
    ///
    /// Sends the given request to the address associated with it.
    ///
    /// Fetches an existing channel with the remote address,
    /// and attempts to send the given request to it.
    ///
    #[inline]
    pub fn send_request(&self, request: Message) {
        let target_addr = request.receiver();
        // Fetch the outbound channel.
        match self.outbound_channel(target_addr) {
            Ok(channel) => match channel.try_send(request) {
                Ok(()) => {
                    metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
                }
                Err(TrySendError::Full(request)) => {
                    warn!(
                        "Couldn't send a {} to {}: the send channel is full",
                        request, target_addr
                    );
                    metrics::increment_counter!(stats::OUTBOUND_ALL_FAILURES);
                }
                Err(TrySendError::Closed(request)) => {
                    error!(
                        "Couldn't send a {} to {}: the send channel is closed",
                        request, target_addr
                    );
                    metrics::increment_counter!(stats::OUTBOUND_ALL_FAILURES);
                }
            },
            Err(_) => {
                warn!("Failed to send a {}: peer is disconnected", request);
                metrics::increment_counter!(stats::OUTBOUND_ALL_FAILURES);
            }
        }
    }

    ///
    /// Establishes an outbound channel to the given remote address, if it does not exist.
    ///
    #[inline]
    fn outbound_channel(&self, remote_address: SocketAddr) -> Result<mpsc::Sender<Message>, NetworkError> {
        self
            .outbound
            .channels
            .get(&remote_address)
            .ok_or(NetworkError::OutboundChannelMissing)
    }

    pub async fn send_ping(&self, remote_address: SocketAddr) {
        // Consider peering tests that don't use the sync layer.
        let current_block_height = if let Some(ref sync) = self.sync() {
            sync.current_block_height()
        } else {
            0
        };

        self.peer_book.sending_ping(remote_address);

        self.send_request(Message::new(
            Direction::Outbound(remote_address),
            Payload::Ping(current_block_height),
        ));
    }

    /// This method handles new outbound messages to a single connected node.
    pub async fn listen_for_outbound_messages(&self, mut receiver: mpsc::Receiver<Message>, writer: &mut ConnWriter) {
        // Read the next message queued to be sent.
        while let Some(message) = receiver.recv().await {
            metrics::decrement_gauge!(stats::QUEUES_OUTBOUND, 1.0);

            match writer.write_message(&message.payload).await {
                Ok(_) => {
                    metrics::increment_counter!(stats::OUTBOUND_ALL_SUCCESSES);
                }
                Err(error) => {
                    warn!("Failed to send a {}: {}", message, error);
                    metrics::increment_counter!(stats::OUTBOUND_ALL_FAILURES);
                }
            }
        }
    }
}
