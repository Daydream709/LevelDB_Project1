// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include <cassert>
#include <limits>

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  static void ReserveForPut(WriteBatch* batch, const Slice& key,
                            const Slice& value) {
    auto varint_length = [](size_t n) {
      size_t length = 1;
      while (n >= 128) { n >>= 7; ++length; }
      return length;
    };
    const size_t prefix = 1 + varint_length(key.size()) + varint_length(value.size());
    const size_t max_size = std::numeric_limits<size_t>::max();
    if (batch->rep_.size() > max_size - prefix) return;
    const size_t overhead = batch->rep_.size() + prefix;
    const size_t limit = max_size - overhead;
    if (key.size() <= limit && value.size() <= limit - key.size()) {
      batch->rep_.reserve(overhead + key.size() + value.size());
    }
  }

  static void ReserveForTTL(WriteBatch* batch, const Slice& key,
                            const Slice& value) {
    const size_t max_size = batch->rep_.max_size();
    if (max_size < 19 || batch->rep_.size() > max_size - 19) return;
    const size_t overhead = batch->rep_.size() + 19;
    if (overhead <= max_size && key.size() <= max_size - overhead &&
        value.size() <= max_size - overhead - key.size()) {
      batch->rep_.reserve(overhead + key.size() + value.size());
    }
  }

  // The DB's ordinary Put creates an empty batch for exactly one value.
  static void AppendSinglePut(WriteBatch* batch, const Slice& key,
                              const Slice& value) {
    assert(batch->rep_.size() == 12 && Count(batch) == 0);
    std::string& out = batch->rep_;
    out[8] = 1;  // The fresh batch count is zero in the other three bytes.
    out.push_back(static_cast<char>(kTypeValue));
    auto append_length = [&out](uint32_t n) {
      while (n >= 128) {
        out.push_back(static_cast<char>(n | 128));
        n >>= 7;
      }
      out.push_back(static_cast<char>(n));
    };
    append_length(static_cast<uint32_t>(key.size()));
    if (!key.empty()) out.append(key.data(), key.size());
    append_length(static_cast<uint32_t>(value.size()));
    if (!value.empty()) out.append(value.data(), value.size());
  }

  static void SetContents(WriteBatch* batch, const Slice& contents);

  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  static void Append(WriteBatch* dst, const WriteBatch* src);

  // Append an internal TTL operation. The deadline is an absolute Env time
  // in microseconds, computed once when the DB accepts the TTL write.
  static void PutWithExpiration(WriteBatch* batch, const Slice& key,
                                const Slice& value,
                                uint64_t expire_time_micros);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
