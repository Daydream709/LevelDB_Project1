// Copyright (c) 2026 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_METADATA_LOG_H_
#define STORAGE_LEVELDB_DB_METADATA_LOG_H_

#include <cstring>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "util/coding.h"

namespace leveldb {

inline unsigned MetadataWALVersion(const Slice& record) {
  static const char v1[] = "LDBMETA1";
  static const char v2[] = "LDBMETA2";
  static const char v3[] = "LDBMETA3";
  if (record.size() >= sizeof(v3) - 1 &&
      std::memcmp(record.data(), v3, sizeof(v3) - 1) == 0) {
    return 3;
  }
  if (record.size() >= sizeof(v2) - 1 &&
      std::memcmp(record.data(), v2, sizeof(v2) - 1) == 0) {
    return 2;
  }
  if (record.size() >= sizeof(v1) - 1 &&
      std::memcmp(record.data(), v1, sizeof(v1) - 1) == 0) {
    return 1;
  }
  return 0;
}

inline bool IsMetadataWALRecord(const Slice& record) {
  return MetadataWALVersion(record) != 0;
}

// Extract and validate the primary WriteBatch stored inside a metadata WAL
// envelope. This is also used by the repair and dump utilities.
inline Status ExtractMetadataWALBatch(const Slice& record, Slice* batch) {
  const unsigned version = MetadataWALVersion(record);
  if (version == 0) {
    return Status::InvalidArgument("not a metadata WAL record");
  }
  Slice input = record;
  input.remove_prefix(sizeof("LDBMETA1") - 1);
  const size_t header_size = version >= 2 ? 32 : 24;
  if (input.size() < header_size) {
    return Status::Corruption("metadata WAL header truncated");
  }
  input.remove_prefix(header_size);
  if (!GetLengthPrefixedSlice(&input, batch)) {
    return Status::Corruption("metadata WAL batch truncated");
  }
  if (version == 3) {
    if (input.empty()) return Status::Corruption("metadata WAL mode missing");
    const unsigned char mode = static_cast<unsigned char>(input[0]);
    input.remove_prefix(1);
    if (mode == 1) {
      if (input.size() != 8) {
        return Status::Corruption("metadata WAL file reference truncated");
      }
      if (DecodeFixed64(input.data()) == 0) {
        return Status::Corruption("metadata WAL file number is zero");
      }
      input.remove_prefix(8);
      return Status::OK();
    }
    if (mode != 0) return Status::Corruption("metadata WAL mode invalid");
  }
  uint32_t updates;
  if (!GetVarint32(&input, &updates) || updates > (1U << 24)) {
    return Status::Corruption("metadata WAL update count");
  }
  for (uint32_t i = 0; i < updates; ++i) {
    Slice key;
    if (!GetLengthPrefixedSlice(&input, &key) || input.size() < 8) {
      return Status::Corruption("metadata WAL update truncated");
    }
    input.remove_prefix(8);  // absolute per-key after-count
  }
  if (!input.empty()) return Status::Corruption("metadata WAL trailing bytes");
  return Status::OK();
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_METADATA_LOG_H_
