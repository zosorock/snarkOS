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

//! Structures for RPC endpoint requests and responses.

use chrono::{DateTime, Utc};
use jsonrpc_core::Metadata;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use snarkos_network::{NodeCluster, NodeType};

/// Defines the authentication format for accessing private endpoints on the RPC server
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RpcCredentials {
    /// The username in the credential
    pub username: String,
    /// The password in the credential
    pub password: String,
}

/// RPC metadata for encoding authentication
#[derive(Default, Clone)]
pub struct Meta {
    /// An optional authentication string for protected RPC functions
    pub auth: Option<String>,
}

impl Metadata for Meta {}

/// Returned value for the `getblock` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockInfo {
    /// Block Hash
    pub hash: String,

    /// Block Height
    pub height: Option<u32>,

    /// Number of confirmations
    pub confirmations: u32,

    /// Block Size
    pub size: usize,

    /// Previous block hash
    pub previous_block_hash: String,

    /// Merkle root representing the transactions in the block
    pub merkle_root: String,

    /// Merkle root of the transactions in the block using a Pedersen hash
    pub pedersen_merkle_root_hash: String,

    /// Proof of Succinct Work
    pub proof: String,

    /// Block time
    pub time: i64,

    /// Block difficulty target
    pub difficulty_target: u64,

    /// Nonce
    pub nonce: u32,

    /// List of transaction ids
    pub transactions: Vec<String>,
}

/// Returned value for the `getblocktemplate` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockTemplate {
    /// Previous block hash
    pub previous_block_hash: String,

    /// Block height
    pub block_height: u32,

    /// Block timestamp
    pub time: i64,

    /// Proof of work difficulty target
    pub difficulty_target: u64,

    /// Transactions to include in the block (excluding the coinbase transaction)
    pub transactions: Vec<String>,

    /// Amount spendable by the coinbase transaction (block rewards + transaction fees)
    pub coinbase_value: u64,
}

/// Output for the `createrawtransaction` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateRawTransactionOuput {
    /// The newly created transaction from calling the `createrawtransaction` endpoint
    pub encoded_transaction: String,
    /// The newly created records from calling the `createrawtransaction` endpoint
    pub encoded_records: Vec<String>,
}

/// Input for the `decryptrecord` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DecryptRecordInput {
    /// The encrypted record
    pub encrypted_record: String,

    /// The account view key used to decrypt the record
    pub account_view_key: String,
}

/// Returned value for the `getnodeinfo` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The configured listening address of the node.
    pub listening_addr: SocketAddr,

    /// Flag indicating if the node's type.
    pub node_type: NodeType,

    /// Flag indicating if the node is operating as a miner
    pub is_miner: bool,

    /// Flag indicating if the node is currently syncing
    pub is_syncing: bool,

    /// The timestamp of when the node was launched.
    pub launched: DateTime<Utc>,

    /// The version of the client binary.
    pub version: String,

    /// The minimum desired number of connected peers.
    pub min_peers: u16,

    /// The maximum allowed number of connected peers.
    pub max_peers: u16,
}

/// Returned value for the `getpeerinfo` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    /// The peers connected to this node
    pub peers: Vec<SocketAddr>,
}

/// Record payload data
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RPCRecordPayload {
    /// Record payload
    pub payload: String,
}

/// Returned value for the `decoderawrecord` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordInfo {
    /// The record owner
    pub owner: String,

    /// Record is dummy flag
    pub is_dummy: bool,

    /// Record value
    pub value: u64,

    /// Record payload
    pub payload: RPCRecordPayload,

    /// Record birth program id
    pub birth_program_id: String,

    /// Record death program id
    pub death_program_id: String,

    /// Record serial number nonce
    pub serial_number_nonce: String,

    /// Record commitment
    pub commitment: String,

    /// Record commitment randomness
    pub commitment_randomness: String,
}

/// Output for the `createaccount` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RpcAccount {
    /// An account private key
    pub private_key: String,
    /// An account view key corresponding to the account private key
    pub view_key: String,
    /// An account address corresponding to the account private key
    pub address: String,
}

/// Returned value for the `gettransaction` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionInfo {
    /// Transaction id
    pub txid: String,

    /// Transaction size
    pub size: usize,

    /// Transaction inputs
    pub old_serial_numbers: Vec<String>,

    /// Transaction outputs
    pub new_commitments: Vec<String>,

    /// Transaction Memo
    pub memo: String,

    /// Network id of the transaction
    pub network_id: u8,

    /// Merkle tree digest
    pub digest: String,

    /// Transaction (outer snark) proof
    pub transaction_proof: String,

    /// Program verification key commitment
    pub program_commitment: String,

    /// Local data root
    pub local_data_root: String,

    /// Transaction value balance
    pub value_balance: i64,

    /// Transaction signatures (Delegated DPC)
    pub signatures: Vec<String>,

    /// Encrypted records
    pub encrypted_records: Vec<String>,

    /// Block the transaction lives in
    pub transaction_metadata: TransactionMetadata,
}

/// Input for the `createrawtransaction` rpc call
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionInputs {
    /// Encoded records that are being spent
    pub old_records: Vec<String>,

    /// Account private keys owning the spent records
    pub old_account_private_keys: Vec<String>,

    /// Transaction recipent and amounts
    pub recipients: Vec<TransactionRecipient>,

    /// Transaction memo
    pub memo: Option<String>,

    /// Network id of the transaction
    pub network_id: u8,
    // Attributes that will be relevant for custom programs
    //    pub new_birth_programs: Vec<String>,
    //    pub new_death_programs: Vec<String>,
    //    pub new_payloads: Vec<String>,
}

/// Additional metadata included with a transaction response
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionMetadata {
    /// The block number associated with this transaction
    pub block_number: Option<u32>,
}

/// Recipient of a transaction
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionRecipient {
    /// The recipient's account address
    pub address: String,
    /// The amount being sent
    pub amount: u64,
}

/// The crawled known network and measurements.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkGraph {
    /// The number of nodes in the known network.
    pub node_count: usize,
    /// The number of connections in the known network.
    pub connection_count: usize,
    /// The density of the network: actual connections divided by the number of possible
    /// connections.
    pub density: f64,
    /// The fiedler eigenvalue.
    pub algebraic_connectivity: f64,
    /// The difference between the node with the largest connection count and the node with the
    /// lowest.
    pub degree_centrality_delta: f64,

    /// The potential height and members of the canon chain tip.
    pub potential_tip: Option<NodeCluster>,
    /// The potential forks in the network and their member nodes.
    pub potential_forks: Vec<NodeCluster>,

    /// Known nodes.
    pub vertices: Vec<Vertice>,
    /// Known connections.
    pub edges: Vec<Edge>,
}

/// Metadata and measurements pertaining to a node in the graph of the known network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Vertice {
    /// The address of the node in the graph.
    pub addr: SocketAddr,
    /// Indicates whether the related node acts as a beacon.
    pub is_beacon: bool,
    /// Indicates whether the related node acts as a sync provider.
    pub is_sync_provider: bool,

    /// The degree of network centrality applicable to the node.
    pub degree_centrality: u16,
    /// The node's eigenvector centrality value in the network.
    pub eigenvector_centrality: f64,
    /// The node's Fiedler value in the network.
    pub fiedler_value: f64,
}

/// A connection in the graph of the known network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// The source of a graph's edge.
    pub source: SocketAddr,
    /// The destination of a graph's edge.
    pub target: SocketAddr,
}
