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

#include "kudu/rpc/rpc_compression.h"

#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <snappy.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using google::protobuf::io::CodedOutputStream;
using strings::Substitute;

DECLARE_int64(rpc_max_decompressed_message_size);
DECLARE_int64(rpc_max_message_size);
DECLARE_int32(rpc_compression_threshold_bytes);
DECLARE_string(rpc_compression_codec);

namespace kudu {
namespace rpc {

namespace {

bool HasPayloadOverCompressionThreshold(
    size_t body_size,
    const std::vector<std::unique_ptr<RpcSidecar>>& sidecars,
    size_t threshold_bytes);
size_t TotalSidecarSize(const std::vector<std::unique_ptr<RpcSidecar>>& sidecars);
size_t RpcRecordSizeLength(uint32_t record_size);
void SerializeRpcBody(uint32_t record_size, const Slice& body, faststring* body_buf);

constexpr int64_t kDefaultMaxDecompressedMessageSize = 256LL * 1024 * 1024;
constexpr size_t kMaxRpcRecordSizeLength = 5;

int64_t EffectiveMaxDecompressedMessageSize() {
  if (FLAGS_rpc_max_decompressed_message_size >= 0) {
    return FLAGS_rpc_max_decompressed_message_size;
  }
  if (FLAGS_rpc_max_message_size >= 0) {
    return FLAGS_rpc_max_message_size;
  }
  return kDefaultMaxDecompressedMessageSize;
}

bool RpcCodecToCompressionType(RpcCompressionCodec rpc_codec,
                               CompressionType* compression) {
  switch (rpc_codec) {
    case RPC_COMPRESSION_CODEC_SNAPPY:
      *compression = SNAPPY;
      return true;
    case RPC_COMPRESSION_CODEC_LZ4:
      *compression = LZ4;
      return true;
    default:
      return false;
  }
}

bool CompressionTypeToRpcCodec(CompressionType compression,
                               RpcCompressionCodec* rpc_codec,
                               RpcFeatureFlag* rpc_feature) {
  switch (compression) {
    case SNAPPY:
      *rpc_codec = RPC_COMPRESSION_CODEC_SNAPPY;
      *rpc_feature = RPC_COMPRESSION_SNAPPY;
      return true;
    case LZ4:
      *rpc_codec = RPC_COMPRESSION_CODEC_LZ4;
      *rpc_feature = RPC_COMPRESSION_LZ4;
      return true;
    default:
      return false;
  }
}

const CompressionCodec* GetCompressionCodecOrNull(CompressionType compression) {
  const CompressionCodec* codec = nullptr;
  Status s = GetCompressionCodec(compression, &codec);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to load RPC compression codec: " << s.ToString();
    return nullptr;
  }
  return codec;
}

template <class Header>
RpcCompressionMetaPB* MutableCompression(Header* header,
                                         RpcCompressionCodec rpc_codec) {
  RpcCompressionMetaPB* compression = header->mutable_compression();
  if (!compression->has_compression_codec()) {
    compression->set_compression_codec(rpc_codec);
  }
  return compression;
}

template <class Header>
size_t ResetSidecarOffsetsForHeader(
    Header* header,
    size_t body_size,
    const std::vector<std::unique_ptr<RpcSidecar>>& sidecars) {
  header->clear_sidecar_offsets();
  size_t offset = body_size;
  size_t sidecar_bytes = 0;
  for (const std::unique_ptr<RpcSidecar>& car : sidecars) {
    CHECK_LE(offset, std::numeric_limits<uint32_t>::max());
    header->add_sidecar_offsets(offset);
    const size_t car_bytes = car->TotalSize();
    CHECK_LE(sidecar_bytes, TransferLimits::kMaxTotalSidecarBytes - car_bytes);
    sidecar_bytes += car_bytes;
    offset += car_bytes;
  }
  return sidecar_bytes;
}

bool KeepCompressedPayload(const CompressionCodec& codec,
                           size_t uncompressed_size,
                           size_t prefix_len,
                           size_t compressed_size,
                           size_t slices_count,
                           faststring* compressed) {
  if (compressed_size == 0 && uncompressed_size != 0) {
    return false;
  }
  if (compressed_size >= uncompressed_size) {
    VLOG(3) << Substitute(
        "Skipping RPC payload compression: codec=$0 uncompressed_size=$1 "
        "compressed_size=$2 slices=$3",
        codec.type(), uncompressed_size, compressed_size, slices_count);
    return false;
  }
  compressed->resize(prefix_len + compressed_size);
  VLOG(2) << Substitute(
      "Compressed RPC payload: codec=$0 uncompressed_size=$1 compressed_size=$2 slices=$3",
      codec.type(), uncompressed_size, compressed_size, slices_count);
  return true;
}

bool TryCompressSlice(const CompressionCodec& codec,
                      const Slice& slice,
                      size_t uncompressed_size,
                      size_t prefix_len,
                      faststring* compressed) {
  if (uncompressed_size > INT_MAX) {
    return false;
  }
  const size_t max_compressed_size = codec.MaxCompressedLength(uncompressed_size);
  if (max_compressed_size == 0) {
    return false;
  }
  compressed->resize(prefix_len + max_compressed_size);
  size_t compressed_size = max_compressed_size;
  Status s = codec.Compress(slice, compressed->data() + prefix_len, &compressed_size);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to compress RPC payload: " << s.ToString();
    return false;
  }
  return KeepCompressedPayload(codec, uncompressed_size, prefix_len,
                               compressed_size, 1, compressed);
}

bool TryCompressSlices(const CompressionCodec& codec,
                       const std::vector<Slice>& slices,
                       size_t uncompressed_size,
                       size_t prefix_len,
                       faststring* compressed) {
  if (uncompressed_size > INT_MAX) {
    return false;
  }
  const size_t max_compressed_size = codec.MaxCompressedLength(uncompressed_size);
  if (max_compressed_size == 0) {
    return false;
  }
  compressed->resize(prefix_len + max_compressed_size);
  size_t compressed_size = max_compressed_size;
  Status s = codec.Compress(slices, compressed->data() + prefix_len, &compressed_size);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to compress RPC payload: " << s.ToString();
    return false;
  }
  return KeepCompressedPayload(codec, uncompressed_size, prefix_len,
                               compressed_size, slices.size(), compressed);
}

template <class Header>
RpcCompressedPayload CompressRpcPayloadsForHeader(
    Header* header,
    const RpcCompressionInfo& comp_info,
    const char* payload_name,
    size_t body_size,
    faststring* body_buf,
    std::vector<std::unique_ptr<RpcSidecar>>* sidecars) {
  const size_t original_sidecar_byte_size = TotalSidecarSize(*sidecars);
  const size_t original_record_size = body_size + original_sidecar_byte_size;
  CHECK_LE(original_record_size, std::numeric_limits<uint32_t>::max());
  const size_t record_size_len =
      RpcRecordSizeLength(static_cast<uint32_t>(original_record_size));

  RpcCompressionMetaPB* compression = nullptr;
  size_t compressed_sidecars_count = 0;
  size_t sidecar_byte_size = 0;
  for (size_t i = 0; i < sidecars->size(); ++i) {
    size_t sidecar_bytes = (*sidecars)[i]->TotalSize();
    if (sidecar_bytes >= comp_info.threshold_bytes) {
      TransferPayload slices;
      (*sidecars)[i]->AppendSlices(&slices);
      faststring compressed;
      const bool compressed_sidecar = slices.size() == 1 ?
          TryCompressSlice(*comp_info.codec, slices[0], sidecar_bytes, 0, &compressed) :
          TryCompressSlices(*comp_info.codec,
                            std::vector<Slice>(slices.begin(), slices.end()),
                            sidecar_bytes,
                            0,
                            &compressed);
      if (compressed_sidecar) {
        compression = MutableCompression(header, comp_info.rpc_codec);
        RpcSidecarCompressionMetaPB* meta = compression->add_compressed_sidecars();
        CHECK_LE(i, std::numeric_limits<uint32_t>::max());
        meta->set_sidecar_index(static_cast<uint32_t>(i));
        CHECK_LE(sidecar_bytes, std::numeric_limits<uint32_t>::max());
        meta->set_uncompressed_size(static_cast<uint32_t>(sidecar_bytes));
        (*sidecars)[i] = RpcSidecar::FromFaststring(std::move(compressed));
        sidecar_bytes = (*sidecars)[i]->TotalSize();
        compressed_sidecars_count++;
      }
    }
    CHECK_LE(sidecar_byte_size, TransferLimits::kMaxTotalSidecarBytes - sidecar_bytes);
    sidecar_byte_size += sidecar_bytes;
  }

  // SerializeMessage() wrote a varint prefix for original_record_size. Strip
  // exactly that prefix to keep the body slice independent of the new size.
  Slice body(body_buf->data() + record_size_len, body_size);
  size_t compressed_body_size = body_size;
  bool body_compressed = false;
  faststring compressed_body;
  if (body_size >= comp_info.threshold_bytes &&
      TryCompressSlice(*comp_info.codec, body, body_size,
                       kMaxRpcRecordSizeLength, &compressed_body)) {
    compression = MutableCompression(header, comp_info.rpc_codec);
    compression->set_uncompressed_body_size(body_size);
    compressed_body_size = compressed_body.size() - kMaxRpcRecordSizeLength;
    body_compressed = true;
  }

  if (compression == nullptr) {
    return { false, sidecar_byte_size };
  }

  CHECK_LE(compressed_body_size, std::numeric_limits<uint32_t>::max() - sidecar_byte_size);
  const size_t compressed_record_size = compressed_body_size + sidecar_byte_size;
  VLOG(1) << Substitute(
      "Compressed RPC $0 payload: codec=$1 original_record_size=$2 "
      "compressed_record_size=$3 body_compressed=$4 compressed_sidecars=$5",
      payload_name, comp_info.rpc_codec, original_record_size, compressed_record_size,
      body_compressed, compressed_sidecars_count);
  if (body_compressed) {
    SerializeRpcBody(static_cast<uint32_t>(compressed_record_size),
                     Slice(compressed_body.data() + kMaxRpcRecordSizeLength,
                           compressed_body_size),
                     &compressed_body);
    *body_buf = std::move(compressed_body);
  } else {
    SerializeRpcBody(static_cast<uint32_t>(compressed_record_size), body, body_buf);
  }

  sidecar_byte_size = ResetSidecarOffsetsForHeader(header, compressed_body_size, *sidecars);
  return { true, sidecar_byte_size };
}

Status GetCompressionCodecForRpcCodec(RpcCompressionCodec rpc_codec,
                                      const CompressionCodec** codec) {
  CompressionType compression;
  if (!RpcCodecToCompressionType(rpc_codec, &compression)) {
    return Status::NotSupported(
        Substitute("unsupported RPC compression codec $0", rpc_codec));
  }
  return GetCompressionCodec(compression, codec);
}

} // anonymous namespace

std::optional<RpcCompressionInfo> GetConfiguredRpcCompressionInfo(
    size_t body_size,
    const std::vector<std::unique_ptr<RpcSidecar>>& sidecars) {
  const int32_t threshold_bytes = FLAGS_rpc_compression_threshold_bytes;
  if (threshold_bytes < 0) {
    return std::nullopt;
  }
  const size_t threshold = threshold_bytes;
  if (!HasPayloadOverCompressionThreshold(body_size, sidecars, threshold)) {
    return std::nullopt;
  }

  const CompressionType compression = GetCompressionCodecType(FLAGS_rpc_compression_codec);
  RpcCompressionCodec rpc_codec;
  RpcFeatureFlag rpc_feature;
  if (!CompressionTypeToRpcCodec(compression, &rpc_codec, &rpc_feature)) {
    return std::nullopt;
  }
  const CompressionCodec* codec = GetCompressionCodecOrNull(compression);
  if (codec == nullptr) {
    return std::nullopt;
  }
  return RpcCompressionInfo { codec, rpc_codec, rpc_feature, threshold };
}

RpcCompressedPayload CompressRpcPayloads(
    RequestHeader* header,
    const RpcCompressionInfo& comp_info,
    const char* payload_name,
    size_t body_size,
    faststring* body_buf,
    std::vector<std::unique_ptr<RpcSidecar>>* sidecars) {
  return CompressRpcPayloadsForHeader(header, comp_info, payload_name, body_size,
                                      body_buf, sidecars);
}

RpcCompressedPayload CompressRpcPayloads(
    ResponseHeader* header,
    const RpcCompressionInfo& comp_info,
    const char* payload_name,
    size_t body_size,
    faststring* body_buf,
    std::vector<std::unique_ptr<RpcSidecar>>* sidecars) {
  return CompressRpcPayloadsForHeader(header, comp_info, payload_name, body_size,
                                      body_buf, sidecars);
}

namespace {

Status UncompressRpcPayload(const CompressionCodec& codec,
                            const Slice& compressed,
                            uint8_t* uncompressed,
                            size_t uncompressed_size) {
  if (codec.type() == SNAPPY) {
    size_t actual_uncompressed_size = 0;
    const bool valid = snappy::GetUncompressedLength(
        reinterpret_cast<const char*>(compressed.data()),
        compressed.size(),
        &actual_uncompressed_size);
    if (!valid) {
      return Status::Corruption("unable to read Snappy uncompressed length");
    }
    if (actual_uncompressed_size != uncompressed_size) {
      return Status::Corruption(Substitute(
          "Snappy uncompressed length $0 does not match expected length $1",
          actual_uncompressed_size, uncompressed_size));
    }
  }
  RETURN_NOT_OK(codec.Uncompress(compressed, uncompressed, uncompressed_size));
  VLOG(2) << Substitute(
      "Decompressed RPC payload: codec=$0 compressed_size=$1 uncompressed_size=$2",
      codec.type(), compressed.size(), uncompressed_size);
  return Status::OK();
}

Status ValidateRpcCompressionMeta(const RpcCompressionMetaPB& compression,
                                  const Slice& body,
                                  const SidecarSliceVector& sidecars,
                                  const char* payload_name) {
  const bool has_compressed_body = compression.has_uncompressed_body_size();
  std::vector<bool> compressed_sidecars_seen(sidecars.size(), false);
  uint64_t uncompressed_payload_size = has_compressed_body ?
      compression.uncompressed_body_size() : body.size();
  for (const RpcSidecarCompressionMetaPB& sidecar_meta :
       compression.compressed_sidecars()) {
    const uint32_t idx = sidecar_meta.sidecar_index();
    if (idx >= sidecars.size()) {
      return Status::Corruption(Substitute(
          "invalid compressed sidecar index $0: $1 has $2 sidecars",
          idx, payload_name, sidecars.size()));
    }
    if (compressed_sidecars_seen[idx]) {
      return Status::Corruption(Substitute(
          "duplicate compressed sidecar index $0", idx));
    }
    compressed_sidecars_seen[idx] = true;
    uncompressed_payload_size += sidecar_meta.uncompressed_size();
  }
  for (size_t i = 0; i < sidecars.size(); ++i) {
    if (!compressed_sidecars_seen[i]) {
      uncompressed_payload_size += sidecars[i].size();
    }
  }
  const int64_t max_decompressed_size = EffectiveMaxDecompressedMessageSize();
  if (uncompressed_payload_size > static_cast<uint64_t>(max_decompressed_size)) {
    return Status::Corruption(Substitute(
        "uncompressed RPC payload size $0 exceeds maximum decompressed RPC message size $1",
        uncompressed_payload_size, max_decompressed_size));
  }
  return Status::OK();
}

} // anonymous namespace

Status DecompressRpcPayload(const RpcCompressionMetaPB& compression,
                            const char* payload_name,
                            Slice* body,
                            SidecarSliceVector* sidecars,
                            faststring* body_buf,
                            std::vector<faststring>* sidecar_bufs) {
  const bool has_compressed_body = compression.has_uncompressed_body_size();
  const bool has_compressed_sidecars = compression.compressed_sidecars_size() > 0;
  if (!has_compressed_body && !has_compressed_sidecars) {
    return Status::OK();
  }

  RETURN_NOT_OK(ValidateRpcCompressionMeta(compression, *body, *sidecars, payload_name));

  const CompressionCodec* codec = nullptr;
  RETURN_NOT_OK(GetCompressionCodecForRpcCodec(compression.compression_codec(), &codec));

  if (has_compressed_body) {
    const uint32_t uncompressed_size = compression.uncompressed_body_size();
    body_buf->resize(uncompressed_size);
    RETURN_NOT_OK_PREPEND(
        UncompressRpcPayload(*codec, *body, body_buf->data(), uncompressed_size),
        Substitute("unable to decompress RPC $0 body", payload_name));
    *body = Slice(*body_buf);
  }

  if (has_compressed_sidecars) {
    sidecar_bufs->resize(sidecars->size());
    for (const RpcSidecarCompressionMetaPB& sidecar_meta :
         compression.compressed_sidecars()) {
      const uint32_t idx = sidecar_meta.sidecar_index();
      const uint32_t uncompressed_size = sidecar_meta.uncompressed_size();
      faststring* uncompressed = &(*sidecar_bufs)[idx];
      uncompressed->resize(uncompressed_size);
      RETURN_NOT_OK_PREPEND(
          UncompressRpcPayload(*codec,
                               (*sidecars)[idx],
                               uncompressed->data(),
                               uncompressed_size),
          Substitute("unable to decompress RPC $0 sidecar $1", payload_name, idx));
      (*sidecars)[idx] = Slice(*uncompressed);
    }
  }

  return Status::OK();
}

namespace {

bool HasPayloadOverCompressionThreshold(
    size_t body_size,
    const std::vector<std::unique_ptr<RpcSidecar>>& sidecars,
    size_t threshold_bytes) {
  if (body_size >= threshold_bytes) {
    return true;
  }
  for (const std::unique_ptr<RpcSidecar>& car : sidecars) {
    if (car->TotalSize() >= threshold_bytes) {
      return true;
    }
  }
  return false;
}

size_t TotalSidecarSize(const std::vector<std::unique_ptr<RpcSidecar>>& sidecars) {
  size_t sidecar_bytes = 0;
  for (const std::unique_ptr<RpcSidecar>& car : sidecars) {
    const size_t car_bytes = car->TotalSize();
    CHECK_LE(sidecar_bytes, TransferLimits::kMaxTotalSidecarBytes - car_bytes);
    sidecar_bytes += car_bytes;
  }
  return sidecar_bytes;
}

size_t RpcRecordSizeLength(uint32_t record_size) {
  return CodedOutputStream::VarintSize32(record_size);
}

void SerializeRpcBody(uint32_t record_size, const Slice& body, faststring* body_buf) {
  const size_t record_size_len = RpcRecordSizeLength(record_size);
  const uint8_t* body_data = body.data();
  const uint8_t* buf_data = body_buf->data();
  const uintptr_t body_addr = reinterpret_cast<uintptr_t>(body_data);
  const uintptr_t buf_addr = reinterpret_cast<uintptr_t>(buf_data);
  const uintptr_t buf_end = buf_addr + body_buf->size();
  if (body_addr >= buf_addr && body_addr <= buf_end &&
      body.size() <= static_cast<size_t>(buf_end - body_addr)) {
    const size_t old_record_size_len = body_addr - buf_addr;
    const size_t new_size = record_size_len + body.size();
    if (record_size_len == old_record_size_len) {
      CodedOutputStream::WriteVarint32ToArray(record_size, body_buf->data());
      body_buf->resize(new_size);
      return;
    }
    if (record_size_len > old_record_size_len) {
      body_buf->resize(new_size);
    }
    memmove(body_buf->data() + record_size_len,
            body_buf->data() + old_record_size_len,
            body.size());
    if (record_size_len <= old_record_size_len) {
      body_buf->resize(new_size);
    }
    CodedOutputStream::WriteVarint32ToArray(record_size, body_buf->data());
    return;
  }

  faststring new_buf;
  new_buf.resize(record_size_len + body.size());
  uint8_t* dst = CodedOutputStream::WriteVarint32ToArray(record_size, new_buf.data());
  memcpy(dst, body.data(), body.size());
  *body_buf = std::move(new_buf);
}

} // anonymous namespace

} // namespace rpc
} // namespace kudu
