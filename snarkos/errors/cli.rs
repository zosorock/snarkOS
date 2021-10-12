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

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),

    #[error("TomlSerError: {0}")]
    TomlSerError(#[from] toml::ser::Error),

    #[error("TomlDeError: {0}")]
    TomlDeError(#[from] toml::de::Error),

    #[error("Only client nodes can run with mining enabled")]
    CantMine,

    #[error("The minimum or maximum value for peer count is invalid")]
    PeerCountInvalid,

    #[error("One of the sync intervals is invalid")]
    SyncIntervalInvalid,
}
