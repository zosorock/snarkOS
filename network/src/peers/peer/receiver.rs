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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::net::TcpStream;

use snarkos_metrics::{self as metrics, connections::*, wrapped_mpsc};

use crate::{NetworkError, Node, Peer, PeerEvent, PeerEventData, PeerHandle, Version};

use super::{network::PeerIOHandle, PeerAction};

impl Peer {
    pub fn receive(
        remote_address: SocketAddr,
        node: Node,
        stream: TcpStream,
        event_target: wrapped_mpsc::Sender<PeerEvent>,
    ) {
        let (sender, receiver) = wrapped_mpsc::channel::<PeerAction>(metrics::queues::OUTBOUND, 64);
        tokio::spawn(async move {
            let (peer, network) = match Peer::inner_receive(remote_address, stream, node.version()).await {
                Err(e) => {
                    error!(
                        "failed to receive incoming connection from peer '{}': '{:?}'",
                        remote_address, e
                    );
                    event_target
                        .send(PeerEvent {
                            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                            data: PeerEventData::FailHandshake,
                        })
                        .await
                        .ok();
                    return;
                }
                Ok(x) => x,
            };

            let mut peer = node.peer_book.fetch_received_peer_data(peer.address).await;

            peer.set_connected();
            metrics::increment_gauge!(CONNECTED, 1.0);
            event_target
                .send(PeerEvent {
                    address: peer.address,
                    data: PeerEventData::Connected(PeerHandle { sender: sender.clone() }),
                })
                .await
                .ok();

            if let Err(e) = peer.run(node, network, receiver).await {
                if !e.is_trivial() {
                    peer.fail();
                    error!(
                        "unrecoverable failure communicating to inbound peer '{}': '{:?}'",
                        peer.address, e
                    );
                } else {
                    warn!(
                        "unrecoverable failure communicating to inbound peer '{}': '{:?}'",
                        peer.address, e
                    );
                }
            }
            metrics::decrement_gauge!(CONNECTED, 1.0);
            peer.set_disconnected();
            event_target
                .send(PeerEvent {
                    address: peer.address,
                    data: PeerEventData::Disconnect(Box::new(peer)),
                })
                .await
                .ok();
        });
    }

    async fn inner_receive(
        remote_address: SocketAddr,
        stream: TcpStream,
        our_version: Version,
    ) -> Result<(Peer, PeerIOHandle), NetworkError> {
        metrics::increment_gauge!(CONNECTING, 1.0);
        let _x = defer::defer(|| metrics::decrement_gauge!(CONNECTING, 1.0));

        Peer::inner_handshake_responder(remote_address, stream, our_version).await
    }
}
