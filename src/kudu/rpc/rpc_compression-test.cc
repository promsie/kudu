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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <google/protobuf/io/coded_stream.h>
#include <gtest/gtest.h>

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_int64(rpc_max_decompressed_message_size);

using google::protobuf::io::CodedInputStream;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

namespace {

RpcCompressionInfo CompressionInfo(CompressionType type,
                                   RpcCompressionCodec rpc_codec,
                                   RpcFeatureFlag rpc_feature,
                                   size_t threshold_bytes) {
  const CompressionCodec* codec = nullptr;
  CHECK_OK(GetCompressionCodec(type, &codec));
  return { codec, rpc_codec, rpc_feature, threshold_bytes };
}

faststring FaststringFromString(const string& data) {
  faststring ret;
  ret.assign_copy(data);
  return ret;
}

string DeterministicRandomBytes(size_t len) {
  string ret;
  ret.resize(len);
  uint32_t state = 0x12345678;
  for (size_t i = 0; i < len; ++i) {
    state = state * 1664525 + 1013904223;
    ret[i] = static_cast<char>(state >> 24);
  }
  return ret;
}

size_t TotalSidecarSize(const vector<unique_ptr<RpcSidecar>>& sidecars) {
  size_t total = 0;
  for (const auto& sidecar : sidecars) {
    total += sidecar->TotalSize();
  }
  return total;
}

void AppendSidecarSlices(const vector<unique_ptr<RpcSidecar>>& sidecars,
                         faststring* dst) {
  TransferPayload slices;
  for (const auto& sidecar : sidecars) {
    sidecar->AppendSlices(&slices);
  }
  for (const Slice& slice : slices) {
    dst->append(slice.data(), slice.size());
  }
}

Slice BodySliceFromSerializedBuffer(const faststring& buf, uint32_t* record_size) {
  CodedInputStream in(buf.data(), buf.size());
  CHECK(in.ReadVarint32(record_size));
  const int body_offset = in.CurrentPosition();
  CHECK_LE(body_offset, buf.size());
  return Slice(buf.data() + body_offset, buf.size() - body_offset);
}

void SerializeTestBody(const string& message,
                       int additional_size,
                       ErrorStatusPB* pb,
                       faststring* body_buf,
                       Slice* body) {
  pb->set_message(message);
  serialization::SerializeMessage(*pb, body_buf, additional_size);
  uint32_t record_size;
  *body = BodySliceFromSerializedBuffer(*body_buf, &record_size);
  ASSERT_EQ(pb->ByteSizeLong() + additional_size, record_size);
  ASSERT_EQ(pb->ByteSizeLong(), body->size());
}

void AddOriginalSidecarOffsets(size_t body_size,
                               const vector<unique_ptr<RpcSidecar>>& sidecars,
                               ResponseHeader* header) {
  size_t offset = body_size;
  for (const auto& sidecar : sidecars) {
    header->add_sidecar_offsets(offset);
    offset += sidecar->TotalSize();
  }
}

void ParseCompressedPayload(const ResponseHeader& header,
                            const faststring& body_buf,
                            const vector<unique_ptr<RpcSidecar>>& sidecars,
                            faststring* wire_payload,
                            Slice* body,
                            SidecarSliceVector* parsed_sidecars) {
  uint32_t record_size;
  Slice wire_body = BodySliceFromSerializedBuffer(body_buf, &record_size);
  wire_payload->assign_copy(wire_body.data(), wire_body.size());
  AppendSidecarSlices(sidecars, wire_payload);
  ASSERT_EQ(record_size, wire_payload->size());
  ASSERT_OK(RpcSidecar::ParseSidecars(header.sidecar_offsets(),
                                      Slice(*wire_payload),
                                      parsed_sidecars));
  *body = header.sidecar_offsets_size() > 0 ?
      Slice(wire_payload->data(), header.sidecar_offsets(0)) :
      Slice(*wire_payload);
}

string SliceToString(const Slice& slice) {
  return string(reinterpret_cast<const char*>(slice.data()), slice.size());
}

} // anonymous namespace

TEST(RpcCompressionTest, CompressesBodyAndSidecarRoundTrip) {
  ErrorStatusPB pb;
  faststring body_buf;
  Slice original_body;
  vector<unique_ptr<RpcSidecar>> sidecars;
  sidecars.emplace_back(RpcSidecar::FromFaststring(
      FaststringFromString(string(80 * 1024, 's'))));
  sidecars.emplace_back(RpcSidecar::FromFaststring(
      FaststringFromString("below-threshold")));
  const int sidecar_bytes = TotalSidecarSize(sidecars);
  SerializeTestBody(string(128 * 1024, 'b'), sidecar_bytes,
                    &pb, &body_buf, &original_body);
  const string original_body_string = SliceToString(original_body);

  ResponseHeader header;
  header.set_call_id(1);
  AddOriginalSidecarOffsets(original_body.size(), sidecars, &header);
  const RpcCompressionInfo comp_info = CompressionInfo(
      LZ4, RPC_COMPRESSION_CODEC_LZ4, RPC_COMPRESSION_LZ4, 1024);

  const RpcCompressedPayload payload = CompressRpcPayloads(
      &header, comp_info, "test response", original_body.size(), &body_buf, &sidecars);

  ASSERT_TRUE(payload.compressed);
  ASSERT_TRUE(header.has_compression());
  ASSERT_EQ(RPC_COMPRESSION_CODEC_LZ4, header.compression().compression_codec());
  ASSERT_TRUE(header.compression().has_uncompressed_body_size());
  ASSERT_EQ(original_body.size(), header.compression().uncompressed_body_size());
  ASSERT_EQ(1, header.compression().compressed_sidecars_size());
  ASSERT_EQ(0, header.compression().compressed_sidecars(0).sidecar_index());
  ASSERT_LT(payload.sidecar_byte_size, sidecar_bytes);

  faststring wire_payload;
  Slice compressed_body;
  SidecarSliceVector parsed_sidecars;
  NO_FATALS(ParseCompressedPayload(header, body_buf, sidecars,
                                   &wire_payload, &compressed_body, &parsed_sidecars));

  faststring decompressed_body_buf;
  vector<faststring> decompressed_sidecar_bufs;
  ASSERT_OK(DecompressRpcPayload(header.compression(),
                                 "test response",
                                 &compressed_body,
                                 &parsed_sidecars,
                                 &decompressed_body_buf,
                                 &decompressed_sidecar_bufs));

  ASSERT_EQ(original_body_string, SliceToString(compressed_body));
  ASSERT_EQ(string(80 * 1024, 's'), SliceToString(parsed_sidecars[0]));
  ASSERT_EQ("below-threshold", SliceToString(parsed_sidecars[1]));
}

TEST(RpcCompressionTest, CompressesSidecarWithoutCompressingSmallBody) {
  ErrorStatusPB pb;
  faststring body_buf;
  Slice original_body;
  vector<unique_ptr<RpcSidecar>> sidecars;
  sidecars.emplace_back(RpcSidecar::FromFaststring(
      FaststringFromString(string(96 * 1024, 'c'))));
  const int sidecar_bytes = TotalSidecarSize(sidecars);
  SerializeTestBody("small-body", sidecar_bytes, &pb, &body_buf, &original_body);
  const string original_body_string = SliceToString(original_body);

  ResponseHeader header;
  header.set_call_id(2);
  AddOriginalSidecarOffsets(original_body.size(), sidecars, &header);
  const RpcCompressionInfo comp_info = CompressionInfo(
      LZ4, RPC_COMPRESSION_CODEC_LZ4, RPC_COMPRESSION_LZ4, 1024);

  const RpcCompressedPayload payload = CompressRpcPayloads(
      &header, comp_info, "test response", original_body.size(), &body_buf, &sidecars);

  ASSERT_TRUE(payload.compressed);
  ASSERT_TRUE(header.has_compression());
  ASSERT_FALSE(header.compression().has_uncompressed_body_size());
  ASSERT_EQ(1, header.compression().compressed_sidecars_size());
  ASSERT_LT(payload.sidecar_byte_size, sidecar_bytes);

  faststring wire_payload;
  Slice body;
  SidecarSliceVector parsed_sidecars;
  NO_FATALS(ParseCompressedPayload(header, body_buf, sidecars,
                                   &wire_payload, &body, &parsed_sidecars));
  ASSERT_EQ(original_body_string, SliceToString(body));

  faststring decompressed_body_buf;
  vector<faststring> decompressed_sidecar_bufs;
  ASSERT_OK(DecompressRpcPayload(header.compression(),
                                 "test response",
                                 &body,
                                 &parsed_sidecars,
                                 &decompressed_body_buf,
                                 &decompressed_sidecar_bufs));
  ASSERT_EQ(original_body_string, SliceToString(body));
  ASSERT_EQ(string(96 * 1024, 'c'), SliceToString(parsed_sidecars[0]));
}

TEST(RpcCompressionTest, CompressesBodyWithSnappyRoundTrip) {
  ErrorStatusPB pb;
  faststring body_buf;
  Slice original_body;
  vector<unique_ptr<RpcSidecar>> sidecars;
  SerializeTestBody(string(128 * 1024, 'z'), 0, &pb, &body_buf, &original_body);
  const string original_body_string = SliceToString(original_body);

  ResponseHeader header;
  header.set_call_id(4);
  const RpcCompressionInfo comp_info = CompressionInfo(
      SNAPPY, RPC_COMPRESSION_CODEC_SNAPPY, RPC_COMPRESSION_SNAPPY, 1024);

  const RpcCompressedPayload payload = CompressRpcPayloads(
      &header, comp_info, "test response", original_body.size(), &body_buf, &sidecars);

  ASSERT_TRUE(payload.compressed);
  ASSERT_TRUE(header.has_compression());
  ASSERT_EQ(RPC_COMPRESSION_CODEC_SNAPPY, header.compression().compression_codec());
  ASSERT_TRUE(header.compression().has_uncompressed_body_size());

  faststring wire_payload;
  Slice compressed_body;
  SidecarSliceVector parsed_sidecars;
  NO_FATALS(ParseCompressedPayload(header, body_buf, sidecars,
                                   &wire_payload, &compressed_body, &parsed_sidecars));

  faststring decompressed_body_buf;
  vector<faststring> decompressed_sidecar_bufs;
  ASSERT_OK(DecompressRpcPayload(header.compression(),
                                 "test response",
                                 &compressed_body,
                                 &parsed_sidecars,
                                 &decompressed_body_buf,
                                 &decompressed_sidecar_bufs));
  ASSERT_EQ(original_body_string, SliceToString(compressed_body));
}

TEST(RpcCompressionTest, RejectsSnappyUncompressedLengthMismatch) {
  ErrorStatusPB pb;
  faststring body_buf;
  Slice original_body;
  vector<unique_ptr<RpcSidecar>> sidecars;
  SerializeTestBody(string(128 * 1024, 'q'), 0, &pb, &body_buf, &original_body);

  ResponseHeader header;
  header.set_call_id(5);
  const RpcCompressionInfo comp_info = CompressionInfo(
      SNAPPY, RPC_COMPRESSION_CODEC_SNAPPY, RPC_COMPRESSION_SNAPPY, 1024);
  const RpcCompressedPayload payload = CompressRpcPayloads(
      &header, comp_info, "test response", original_body.size(), &body_buf, &sidecars);
  ASSERT_TRUE(payload.compressed);

  faststring wire_payload;
  Slice compressed_body;
  SidecarSliceVector parsed_sidecars;
  NO_FATALS(ParseCompressedPayload(header, body_buf, sidecars,
                                   &wire_payload, &compressed_body, &parsed_sidecars));

  RpcCompressionMetaPB bad_compression = header.compression();
  bad_compression.set_uncompressed_body_size(original_body.size() + 1);
  faststring decompressed_body_buf;
  vector<faststring> decompressed_sidecar_bufs;
  const Status s = DecompressRpcPayload(bad_compression,
                                        "test response",
                                        &compressed_body,
                                        &parsed_sidecars,
                                        &decompressed_body_buf,
                                        &decompressed_sidecar_bufs);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Snappy uncompressed length");
}

TEST(RpcCompressionTest, SkipsIncompressiblePayload) {
  ErrorStatusPB pb;
  faststring body_buf;
  Slice original_body;
  vector<unique_ptr<RpcSidecar>> sidecars;
  SerializeTestBody(DeterministicRandomBytes(64 * 1024), 0,
                    &pb, &body_buf, &original_body);
  const string original_buf = body_buf.ToString();

  ResponseHeader header;
  header.set_call_id(3);
  const RpcCompressionInfo comp_info = CompressionInfo(
      LZ4, RPC_COMPRESSION_CODEC_LZ4, RPC_COMPRESSION_LZ4, 1);

  const RpcCompressedPayload payload = CompressRpcPayloads(
      &header, comp_info, "test response", original_body.size(), &body_buf, &sidecars);

  ASSERT_FALSE(payload.compressed);
  ASSERT_FALSE(header.has_compression());
  ASSERT_EQ(0, payload.sidecar_byte_size);
  ASSERT_EQ(original_buf, body_buf.ToString());
}

TEST(RpcCompressionTest, RejectsInvalidCompressedSidecarMetadata) {
  RpcCompressionMetaPB compression;
  compression.set_compression_codec(RPC_COMPRESSION_CODEC_LZ4);
  RpcSidecarCompressionMetaPB* meta = compression.add_compressed_sidecars();
  meta->set_sidecar_index(1);
  meta->set_uncompressed_size(1024);

  Slice body;
  SidecarSliceVector sidecars;
  sidecars.emplace_back(Slice(reinterpret_cast<const uint8_t*>("x"), 1));
  faststring body_buf;
  vector<faststring> sidecar_bufs;

  const Status s = DecompressRpcPayload(compression,
                                        "test response",
                                        &body,
                                        &sidecars,
                                        &body_buf,
                                        &sidecar_bufs);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST(RpcCompressionTest, RejectsPayloadOverDecompressedSizeLimit) {
  const int64_t old_limit = FLAGS_rpc_max_decompressed_message_size;
  FLAGS_rpc_max_decompressed_message_size = 1;

  RpcCompressionMetaPB compression;
  compression.set_compression_codec(RPC_COMPRESSION_CODEC_LZ4);
  compression.set_uncompressed_body_size(2);

  Slice body(reinterpret_cast<const uint8_t*>("x"), 1);
  SidecarSliceVector sidecars;
  faststring body_buf;
  vector<faststring> sidecar_bufs;
  const Status s = DecompressRpcPayload(compression,
                                        "test response",
                                        &body,
                                        &sidecars,
                                        &body_buf,
                                        &sidecar_bufs);
  FLAGS_rpc_max_decompressed_message_size = old_limit;

  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

} // namespace rpc
} // namespace kudu
