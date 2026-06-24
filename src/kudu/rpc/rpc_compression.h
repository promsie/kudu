// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/status.h"

namespace kudu {

class CompressionCodec;
class Slice;
class faststring;

namespace rpc {

struct RpcCompressionInfo {
  const CompressionCodec* codec;
  RpcCompressionCodec rpc_codec;
  RpcFeatureFlag rpc_feature;
  size_t threshold_bytes;
};

struct RpcCompressedPayload {
  bool compressed;
  size_t sidecar_byte_size;
};

std::optional<RpcCompressionInfo> GetConfiguredRpcCompressionInfo(
    size_t body_size,
    const std::vector<std::unique_ptr<RpcSidecar>>& sidecars);

RpcCompressedPayload CompressRpcPayloads(
    RequestHeader* header,
    const RpcCompressionInfo& comp_info,
    const char* payload_name,
    size_t body_size,
    faststring* body_buf,
    std::vector<std::unique_ptr<RpcSidecar>>* sidecars);

RpcCompressedPayload CompressRpcPayloads(
    ResponseHeader* header,
    const RpcCompressionInfo& comp_info,
    const char* payload_name,
    size_t body_size,
    faststring* body_buf,
    std::vector<std::unique_ptr<RpcSidecar>>* sidecars);

Status DecompressRpcPayload(const RpcCompressionMetaPB& compression,
                            const char* payload_name,
                            Slice* body,
                            SidecarSliceVector* sidecars,
                            faststring* body_buf,
                            std::vector<faststring>* sidecar_bufs);

} // namespace rpc
} // namespace kudu
