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

use crate::Message;
use snarkos_consensus::error::ConsensusError;
use snarkvm_dpc::{BlockError, StorageError};

use std::{fmt, io::ErrorKind};

#[derive(Debug)]
pub enum NetworkError {
    Bincode(Box<bincode::ErrorKind>),
    BlockError(BlockError),
    CapnProto(capnp::Error),
    ConsensusError(ConsensusError),
    HandshakeTimeout,
    Io(std::io::Error),
    InvalidHandshake,
    MessageTooBig(usize),
    Noise(snow::error::Error),
    PeerAlreadyConnected,
    PeerAlreadyConnecting,
    PeerAlreadyDisconnected,
    PeerBookFailedToLoad,
    PeerBookIsCorrupt,
    PeerBookMissingPeer,
    PeerCountInvalid,
    PeerIsDisconnected,
    SelfConnectAttempt,
    SenderError(tokio::sync::mpsc::error::SendError<Message>),
    TaskPanicked(tokio::task::JoinError),
    TooManyConnections,
    OutboundChannelMissing,
    ReceiverFailedToParse,
    StorageError(StorageError),
    SyncIntervalInvalid,
    ZeroLengthMessage,
    Other(anyhow::Error),
}

impl NetworkError {
    // FIXME (nkls): is unused and overlaps with `is_trivial`?
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::Io(err) => [
                ErrorKind::BrokenPipe,
                ErrorKind::ConnectionReset,
                ErrorKind::UnexpectedEof,
            ]
            .contains(&err.kind()),
            // other critical errors
            Self::CapnProto(_) | Self::MessageTooBig(..) | Self::ZeroLengthMessage | Self::Noise(_) => true,
            _ => false,
        }
    }

    pub fn is_trivial(&self) -> bool {
        match self {
            Self::Io(e) => {
                matches!(
                    e.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::TimedOut
                        | ErrorKind::ConnectionRefused
                )
            }
            _ => false,
        }
    }
}

impl From<capnp::Error> for NetworkError {
    fn from(error: capnp::Error) -> Self {
        NetworkError::CapnProto(error)
    }
}

impl From<snow::Error> for NetworkError {
    fn from(error: snow::Error) -> Self {
        NetworkError::Noise(error)
    }
}

impl From<BlockError> for NetworkError {
    fn from(error: BlockError) -> Self {
        NetworkError::BlockError(error)
    }
}

impl From<ConsensusError> for NetworkError {
    fn from(error: ConsensusError) -> Self {
        NetworkError::ConsensusError(error)
    }
}

impl From<StorageError> for NetworkError {
    fn from(error: StorageError) -> Self {
        NetworkError::StorageError(error)
    }
}

impl From<Box<bincode::ErrorKind>> for NetworkError {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        NetworkError::Bincode(error)
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(error: std::io::Error) -> Self {
        NetworkError::Io(error)
    }
}

impl From<tokio::sync::mpsc::error::SendError<Message>> for NetworkError {
    fn from(error: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        NetworkError::SenderError(error)
    }
}

impl From<tokio::task::JoinError> for NetworkError {
    fn from(error: tokio::task::JoinError) -> Self {
        NetworkError::TaskPanicked(error)
    }
}

impl From<NetworkError> for anyhow::Error {
    fn from(error: NetworkError) -> Self {
        error!("{}", error);
        Self::msg(error.to_string())
    }
}

impl From<anyhow::Error> for NetworkError {
    fn from(other: anyhow::Error) -> Self {
        Self::Other(other)
    }
}
