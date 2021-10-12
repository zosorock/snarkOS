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

//! Logic for instantiating the RPC server.

use crate::{
    rpc_trait::RpcFunctions,
    rpc_types::{Meta, RpcCredentials},
    RpcImpl,
};
use snarkos_metrics::{self as metrics, misc};
use snarkos_network::Node;
use snarkos_storage::DynStorage;

use hyper::{
    body::HttpBody,
    server::Server,
    service::{make_service_fn, service_fn},
    Body,
};
use json_rpc_types as jrt;
use jsonrpc_core::Params;
use serde::Serialize;

use std::{convert::Infallible, net::SocketAddr};

const METHODS_EXPECTING_PARAMS: [&str; 15] = [
    // public
    "getblock",
    "getblockhash",
    "getrawtransaction",
    "gettransactioninfo",
    "decoderawtransaction",
    "sendtransaction",
    "validaterawtransaction",
    // private
    "createrawtransaction",
    "createtransactionkernel",
    "createtransaction",
    "getrawrecord",
    "decoderecord",
    "decryptrecord",
    "disconnect",
    "connect",
];

#[allow(clippy::too_many_arguments)]
/// Starts a local RPC HTTP server at `rpc_port` in a dedicated `tokio` task.
/// RPC failures do not affect the rest of the node.
pub fn start_rpc_server(
    rpc_addr: SocketAddr,
    storage: DynStorage,
    node_server: Node,
    username: Option<String>,
    password: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let credentials = match (username, password) {
        (Some(username), Some(password)) => Some(RpcCredentials { username, password }),
        _ => None,
    };

    let rpc_impl = RpcImpl::new(storage, credentials, node_server);

    let service = make_service_fn(move |_conn| {
        let rpc = rpc_impl.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| handle_rpc(rpc.clone(), req))) }
    });

    let server = Server::bind(&rpc_addr).serve(service);

    tokio::spawn(async move {
        server.await.expect("The RPC server couldn't be started!");
    })
}

async fn handle_rpc(rpc: RpcImpl, req: hyper::Request<Body>) -> Result<hyper::Response<Body>, Infallible> {
    // Register the request in the metrics.
    metrics::increment_counter!(misc::RPC_REQUESTS);

    // Obtain the username and password, if present.
    let auth = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .map(|h| h.to_str().unwrap_or("").to_owned());
    let meta = Meta { auth };

    // Ready the body of the request
    let mut body = req.into_body();
    let data = match body.data().await {
        Some(Ok(data)) => data,
        err_or_none => {
            let mut error = jrt::Error::with_custom_msg(jrt::ErrorCode::ParseError, "Couldn't read the RPC body");
            if let Some(Err(err)) = err_or_none {
                error.data = Some(err.to_string());
            }

            let resp = jrt::Response::<(), String>::error(jrt::Version::V2, error, None);
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Deserialize the JSON-RPC request.
    let req: jrt::Request<Params> = match serde_json::from_slice(&data) {
        Ok(req) => req,
        Err(_) => {
            let resp = jrt::Response::<(), ()>::error(
                jrt::Version::V2,
                jrt::Error::with_custom_msg(jrt::ErrorCode::ParseError, "Couldn't parse the RPC body"),
                None,
            );
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Read the request params.
    let mut params = match read_params(&req) {
        Ok(params) => params,
        Err(err) => {
            let resp = jrt::Response::<(), ()>::error(jrt::Version::V2, err, req.id.clone());
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Handle the request method.
    let response = match &*req.method {
        // public
        "getblock" => {
            let result = rpc
                .get_block(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblockcount" => {
            let result = rpc.get_block_count().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getbestblockhash" => {
            let result = rpc.get_best_block_hash().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblockhash" => match serde_json::from_value::<u32>(params.remove(0)) {
            Ok(height) => {
                let result = rpc.get_block_hash(height).await.map_err(convert_crate_err);
                result_to_response(&req, result)
            }
            Err(_) => {
                let err = jrt::Error::with_custom_msg(jrt::ErrorCode::ParseError, "Invalid block height!");
                jrt::Response::error(jrt::Version::V2, err, req.id.clone())
            }
        },
        "getrawtransaction" => {
            let result = rpc
                .get_raw_transaction(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "gettransactioninfo" => {
            let result = rpc
                .get_transaction_info(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "decoderawtransaction" => {
            let result = rpc
                .decode_raw_transaction(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "sendtransaction" => {
            let result = rpc
                .send_raw_transaction(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "validaterawtransaction" => {
            let result = rpc
                .validate_raw_transaction(params[0].as_str().unwrap_or("").into())
                .await
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getconnectioncount" => {
            let result = rpc.get_connection_count().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getpeerinfo" => {
            let result = rpc.get_peer_info().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getnodeinfo" => {
            let result = rpc.get_node_info().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getnodestats" => {
            let result = rpc.get_node_stats().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblocktemplate" => {
            let result = rpc.get_block_template().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getnetworkgraph" => {
            let result = rpc.get_network_graph().await.map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        // private
        "createaccount" => {
            let result = rpc
                .create_account_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createrawtransaction" => {
            let result = rpc
                .create_raw_transaction_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createtransactionkernel" => {
            let result = rpc
                .create_transaction_kernel_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createtransaction" => {
            let result = rpc
                .create_transaction_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "getrecordcommitments" => {
            let result = rpc
                .get_record_commitments_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "getrawrecord" => {
            let result = rpc
                .get_raw_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "decoderecord" => {
            let result = rpc
                .decode_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "decryptrecord" => {
            let result = rpc
                .decrypt_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "disconnect" => {
            let result = rpc
                .disconnect_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "connect" => {
            let result = rpc
                .connect_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        _ => {
            let err = jrt::Error::from_code(jrt::ErrorCode::MethodNotFound);
            jrt::Response::error(jrt::Version::V2, err, req.id.clone())
        }
    };

    // Serialize the response object.
    let body = serde_json::to_vec(&response).unwrap_or_default();

    // Send the HTTP response.
    Ok(hyper::Response::new(body.into()))
}

/// Ensures that the params are a non-empty (this assumption is taken advantage of later) array and returns them.
fn read_params(req: &jrt::Request<Params>) -> Result<Vec<serde_json::Value>, jrt::Error<()>> {
    if METHODS_EXPECTING_PARAMS.contains(&&*req.method) {
        match &req.params {
            Some(Params::Array(arr)) if !arr.is_empty() => Ok(arr.clone()),
            Some(_) => Err(jrt::Error::from_code(jrt::ErrorCode::InvalidParams)),
            None => Err(jrt::Error::from_code(jrt::ErrorCode::InvalidParams)),
        }
    } else {
        Ok(vec![]) // unused in methods other than METHODS_EXPECTING_PARAMS
    }
}

/// Converts the crate's RpcError into a jrt::RpcError
fn convert_crate_err(err: crate::error::RpcError) -> jrt::Error<String> {
    let error = jrt::Error::with_custom_msg(jrt::ErrorCode::ServerError(-32000), "internal error");
    error.set_data(err.to_string())
}

/// Converts the jsonrpc-core's Error into a jrt::RpcError
fn convert_core_err(err: jsonrpc_core::Error) -> jrt::Error<String> {
    let error = jrt::Error::with_custom_msg(jrt::ErrorCode::InternalError, "JSONRPC server error");
    error.set_data(err.to_string())
}

fn result_to_response<T: Serialize>(
    request: &jrt::Request<Params>,
    result: Result<T, jrt::Error<String>>,
) -> jrt::Response<serde_json::Value, String> {
    match result {
        Ok(res) => {
            let result = serde_json::to_value(&res).unwrap_or_default();
            jrt::Response::result(jrt::Version::V2, result, request.id.clone())
        }
        Err(err) => jrt::Response::error(jrt::Version::V2, err, request.id.clone()),
    }
}
