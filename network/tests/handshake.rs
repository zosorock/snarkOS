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

use snarkos_network::{message::*, Node, Version};
use snarkos_storage::Digest;
use snarkos_testing::{
    network::{test_node, write_message_to_stream, TestSetup},
    wait_until,
};

use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

#[tokio::test]
async fn handshake_responder_side() {
    // start a test node and listen for incoming connections
    let setup = TestSetup {
        consensus_setup: None,
        ..Default::default()
    };
    let node = test_node(setup).await;
    let node_listener = node.expect_local_addr();

    // set up a fake node (peer), which is just a socket
    let mut peer_stream = TcpStream::connect(&node_listener).await.unwrap();

    // register the addresses bound to the connection between the node and the peer
    let peer_address = peer_stream.local_addr().unwrap();

    let builder = snow::Builder::with_resolver(
        snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
        Box::new(snow::resolvers::SodiumResolver),
    );
    let static_key = builder.generate_keypair().unwrap().private;
    let noise_builder = builder
        .local_private_key(&static_key)
        .psk(3, snarkos_network::HANDSHAKE_PSK);
    let mut noise = noise_builder.build_initiator().unwrap();
    let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::NOISE_BUF_LEN].into();
    let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN]; // a temporary intermediate buffer to decrypt from

    wait_until!(1, node.peer_book.pending_connections() == 1);

    // -> e
    let len = noise.write_message(&[], &mut buffer).unwrap();
    peer_stream.write_all(&[len as u8]).await.unwrap();
    peer_stream.write_all(&buffer[..len]).await.unwrap();

    // <- e, ee, s, es
    peer_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = peer_stream.read_exact(&mut buf[..len]).await.unwrap();
    let len = noise.read_message(&buf[..len], &mut buffer).unwrap();
    let _node_version = Version::deserialize(&buffer[..len]).unwrap();

    // -> s, se, psk
    let peer_version =
        Version::serialize(&Version::new(snarkos_network::PROTOCOL_VERSION, peer_address.port(), 0)).unwrap();
    let len = noise.write_message(&peer_version, &mut buffer).unwrap();
    peer_stream.write_all(&[len as u8]).await.unwrap();
    peer_stream.write_all(&buffer[..len]).await.unwrap();

    // the node should now have register the peer as 'connected'
    sleep(Duration::from_millis(200)).await;
    assert!(node.peer_book.is_connected(peer_address));
    assert_eq!(node.peer_book.get_active_peer_count(), 1);
}

#[tokio::test]
async fn handshake_initiator_side() {
    // start a fake peer which is just a socket
    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_address = peer_listener.local_addr().unwrap();

    // note: using the smallest allowed interval for peer sync
    let setup = TestSetup {
        consensus_setup: None,
        peer_sync_interval: 1,
        ..Default::default()
    };
    let node = test_node(setup).await;
    node.connect_to_addresses(&[peer_address]).await;

    // accept the node's connection on peer side
    let (mut peer_stream, _node_address) = peer_listener.accept().await.unwrap();

    wait_until!(1, node.peer_book.pending_connections() == 1);

    let builder = snow::Builder::with_resolver(
        snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
        Box::new(snow::resolvers::SodiumResolver),
    );
    let static_key = builder.generate_keypair().unwrap().private;
    let noise_builder = builder
        .local_private_key(&static_key)
        .psk(3, snarkos_network::HANDSHAKE_PSK);
    let mut noise = noise_builder.build_responder().unwrap();
    let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::NOISE_BUF_LEN].into();
    let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN]; // a temporary intermediate buffer to decrypt from

    // <- e
    peer_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = peer_stream.read_exact(&mut buf[..len]).await.unwrap();
    noise.read_message(&buf[..len], &mut buffer).unwrap();

    // -> e, ee, s, es
    let peer_version =
        Version::serialize(&Version::new(snarkos_network::PROTOCOL_VERSION, peer_address.port(), 0)).unwrap();
    let len = noise.write_message(&peer_version, &mut buffer).unwrap();
    peer_stream.write_all(&[len as u8]).await.unwrap();
    peer_stream.write_all(&buffer[..len]).await.unwrap();

    // <- s, se, psk
    peer_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = peer_stream.read_exact(&mut buf[..len]).await.unwrap();
    let len = noise.read_message(&buf[..len], &mut buffer).unwrap();
    let _node_version = Version::deserialize(&buffer[..len]).unwrap();

    // the node should now have registered the peer as 'connected'
    sleep(Duration::from_millis(200)).await;
    assert!(node.peer_book.is_connected(peer_address));
    assert_eq!(node.peer_book.get_active_peer_count(), 1);
}

async fn assert_node_rejected_message(node: &Node, peer_stream: &mut TcpStream) {
    // read the response from the stream
    let mut buffer = String::new();
    let bytes_read = peer_stream.read_to_string(&mut buffer).await.unwrap();

    // check node's response is empty
    assert_eq!(bytes_read, 0);
    assert!(buffer.is_empty());

    // check the node's state hasn't been altered by the message
    wait_until!(1, !node.peer_book.is_connected(peer_stream.local_addr().unwrap()));
    assert_eq!(node.peer_book.get_active_peer_count(), 0);
}

#[tokio::test]
async fn reject_non_version_messages_before_handshake() {
    // start the node
    let setup = TestSetup {
        consensus_setup: None,
        ..Default::default()
    };
    let node = test_node(setup).await;

    // start the fake node (peer) which is just a socket
    // note: the connection needs to be re-established as it is reset
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();

    // send a GetPeers message without a prior handshake established
    write_message_to_stream(Payload::GetPeers, &mut peer_stream).await;

    // verify the node rejected the message, the response to the peer is empty and the node's
    // state is unaltered
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // GetMemoryPool
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    write_message_to_stream(Payload::GetMemoryPool, &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // GetBlock
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let block_hash = Digest::from([0u8; 32]);
    write_message_to_stream(Payload::GetBlocks(vec![block_hash]), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // GetSync
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let block_hash = Digest::from([0u8; 32]);
    write_message_to_stream(Payload::GetSync(vec![block_hash]), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // Peers
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let peers = vec!["127.0.0.1:0".parse().unwrap()];
    write_message_to_stream(Payload::Peers(peers), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // MemoryPool
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let memory_pool = vec![vec![0u8, 10]];
    write_message_to_stream(Payload::MemoryPool(memory_pool), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // Block
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let block = vec![0u8, 10];
    let height = None;
    write_message_to_stream(Payload::Block(block, height), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // SyncBlock
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let sync_block = vec![0u8, 10];
    let height = Some(1);
    write_message_to_stream(Payload::SyncBlock(sync_block, height), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // Sync
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let block_hash = Digest::from([0u8; 32]);
    write_message_to_stream(Payload::Sync(vec![block_hash]), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;

    // Transaction
    let mut peer_stream = TcpStream::connect(node.expect_local_addr()).await.unwrap();
    let transaction = vec![0u8, 10];
    write_message_to_stream(Payload::Transaction(transaction), &mut peer_stream).await;
    assert_node_rejected_message(&node, &mut peer_stream).await;
}

#[tokio::test]
async fn handshake_timeout_initiator_side() {
    const NUM_BEACONS: usize = 5;

    // set up beacons that won't perform a valid handshake
    let mut failing_beacons = Vec::with_capacity(NUM_BEACONS);
    for _ in 0..NUM_BEACONS {
        failing_beacons.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }

    // start the node
    let setup = TestSetup {
        consensus_setup: None,
        beacons: failing_beacons
            .iter()
            .map(|l| {
                let addr = l.local_addr().unwrap();
                format!("{}:{}", addr.ip(), addr.port())
            })
            .collect(),
        ..Default::default()
    };
    let node = test_node(setup).await;

    // the node should start connecting to all the configured beacons.
    wait_until!(3, node.peer_book.get_active_peer_count() != 0);

    // but since they won't reply, it should drop them after the handshake deadline
    wait_until!(
        snarkos_network::HANDSHAKE_TIMEOUT_SECS as u64 + 1,
        node.peer_book.get_active_peer_count() == 0
    );
}

#[tokio::test]
async fn handshake_timeout_responder_side() {
    // start the node
    let setup = TestSetup {
        consensus_setup: None,
        ..Default::default()
    };
    let node = test_node(setup).await;
    let node_addr = node.expect_local_addr();

    // set up a "peer" that won't perform a valid handshake
    let _fake_peer = TcpStream::connect(node_addr).await.unwrap();

    // the node should initally accept the connection
    wait_until!(3, node.peer_book.get_active_peer_count() == 1u32);

    // but since it won't conclude the handshake, it should be dropped after the handshake deadline
    wait_until!(
        snarkos_network::HANDSHAKE_TIMEOUT_SECS as u64 + 1,
        node.peer_book.get_active_peer_count() == 0
    );
}
