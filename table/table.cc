// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"
#include "util/profiler.h"

#include <cstring>
#include <memory>
#include <optional>

#include <limits>

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  TableInternalStats internal_stats;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation.
    ProfileScope::Count("metadata.metaindex_unreadable");
    return;
  }
  Block* meta = new Block(contents);
  bool found_stats = false;

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (rep_->options.filter_policy != nullptr) {
      std::string filter_key = "filter.";
      filter_key.append(rep_->options.filter_policy->Name());
      if (iter->key() == Slice(filter_key)) ReadFilter(iter->value());
    }
    if (iter->key() == Slice("leveldb.internal-stats.v1")) {
      found_stats = true;
      ReadInternalStats(iter->value());
    }
  }
  if (!found_stats) ProfileScope::Count("metadata.index_missing");
  delete iter;
  delete meta;
}

void Table::ReadInternalStats(const Slice& stats_handle_value) {
  ProfileScope profile("metadata.load_index");
  const char* outcome = [&]() -> const char* {
  rep_->internal_stats.valid = false;
  Slice handle_input = stats_handle_value;
  BlockHandle handle;
  if (!handle.DecodeFrom(&handle_input).ok()) return "invalid";

  ReadOptions opt;
  opt.verify_checksums = true;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, handle, &contents).ok()) return "invalid";
  std::unique_ptr<char[]> owner;
  if (contents.heap_allocated) {
    owner.reset(const_cast<char*>(contents.data.data()));
  }
  if (contents.data.size() < 4) return "invalid";

  const size_t payload_size = contents.data.size() - 4;
  const uint32_t checksum = DecodeFixed32(contents.data.data() + payload_size);
  if (crc32c::Value(contents.data.data(), payload_size) != checksum) return "checksum_error";

  ProfileScope::Count("metadata.loaded_index_bytes", contents.data.size());
  ProfileScope parsing("metadata.parse_index");
  Slice input(contents.data.data(), payload_size);
  if (input.size() < 4 || std::memcmp(input.data(), "LDS1", 4) != 0) return "invalid";
  input.remove_prefix(4);
  if (input.size() < 4 + 8 * 4 + 16 * 8) return "invalid";
  const uint32_t version = DecodeFixed32(input.data());
  input.remove_prefix(4);
  if (version != 1 && version != 2) return "unsupported";

  TableInternalStats stats;
  stats.total_records = DecodeFixed64(input.data());
  input.remove_prefix(8);
  stats.ttl_records = DecodeFixed64(input.data());
  input.remove_prefix(8);
  stats.min_expire_time_micros = DecodeFixed64(input.data());
  input.remove_prefix(8);
  stats.max_expire_time_micros = DecodeFixed64(input.data());
  input.remove_prefix(8);
  uint64_t histogram_total = 0;
  for (uint64_t& count : stats.expiration_histogram) {
    count = DecodeFixed64(input.data());
    input.remove_prefix(8);
    histogram_total += count;
  }

  uint32_t key_count = 0;
  if (!GetVarint32(&input, &key_count)) return "invalid";
  if (version == 2 && key_count != 0) return "invalid";
  uint64_t record_total = 0;
  std::string previous_key;
  for (uint32_t i = 0; i < key_count; ++i) {
    Slice key;
    if (!GetLengthPrefixedSlice(&input, &key) || input.size() < 8) return "invalid";
    const uint64_t count = DecodeFixed64(input.data());
    input.remove_prefix(8);
    if (count == 0 ||
        (i != 0 && BytewiseComparator()->Compare(previous_key, key) >= 0) ||
        record_total > UINT64_MAX - count) {
      return "invalid";
    }
    previous_key = key.ToString();
    record_total += count;
  }
  if (!input.empty() || (version == 1 && record_total != stats.total_records) ||
      stats.ttl_records > stats.total_records ||
      histogram_total != stats.ttl_records) {
    return "invalid";
  }
  if (stats.ttl_records == 0) {
    if (stats.min_expire_time_micros != 0 ||
        stats.max_expire_time_micros != 0) {
      return "invalid";
    }
  } else if (stats.min_expire_time_micros > stats.max_expire_time_micros) {
    return "invalid";
  }
  stats.valid = true;
  rep_->internal_stats = std::move(stats);

  return "valid";

  }();
  ProfileScope::Count(std::string("metadata.index_") + outcome);
  if (!rep_->internal_stats.valid) profile.SetStatus(Status::Corruption("optional statistics index"));
}

Status Table::GetInternalStats(TableInternalStats* stats) const {
  if (stats == nullptr) {
    return Status::InvalidArgument("table statistics output pointer is null");
  }
  *stats = rep_->internal_stats;
  return Status::OK();
}

Status Table::GetTTLStats(TableInternalStats* stats) const {
  if (stats == nullptr) {
    return Status::InvalidArgument("table statistics output pointer is null");
  }
  stats->total_records = rep_->internal_stats.total_records;
  stats->ttl_records = rep_->internal_stats.ttl_records;
  stats->min_expire_time_micros =
      rep_->internal_stats.min_expire_time_micros;
  stats->max_expire_time_micros =
      rep_->internal_stats.max_expire_time_micros;
  for (size_t i = 0; i < 16; ++i) {
    stats->expiration_histogram[i] =
        rep_->internal_stats.expiration_histogram[i];
  }
  stats->valid = rep_->internal_stats.valid;
  return Status::OK();
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&),
                          bool bytewise_internal) {
  struct IndexResult {
    Slice value;
    bool found = false;
  } index_result;
  Status s = rep_->index_block->SeekThen(
      rep_->options.comparator, k, &index_result,
      [](void* arg, const Slice&, const Slice& value) {
        IndexResult* result = static_cast<IndexResult*>(arg);
        result->value = value;
        result->found = true;
      }, bytewise_internal);
  if (s.ok() && index_result.found) {
    Slice handle_value = index_result.value;
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      // A point lookup does not need to retain a heap iterator. Keep the
      // cache reference (or owned block) alive until the callback returns.
      Slice input = index_result.value;
      BlockHandle data_handle;
      s = data_handle.DecodeFrom(&input);
      Cache* block_cache = rep_->options.block_cache;
      Cache::Handle* cache_handle = nullptr;
      Block* block = nullptr;
      std::optional<Block> mapped_block;
      if (s.ok()) {
        BlockContents contents;
        char cache_key_buffer[16];
        if (block_cache != nullptr) {
          EncodeFixed64(cache_key_buffer, rep_->cache_id);
          EncodeFixed64(cache_key_buffer + 8, data_handle.offset());
          cache_handle = block_cache->Lookup(Slice(cache_key_buffer, 16));
        }
        if (cache_handle != nullptr) {
          block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
        } else {
          s = ReadBlock(rep_->file, options, data_handle, &contents);
          if (s.ok()) {
            if (!contents.heap_allocated && !contents.cachable) {
              mapped_block.emplace(contents);
              block = &*mapped_block;
            } else {
              block = new Block(contents);
            }
            if (block_cache != nullptr && contents.cachable && options.fill_cache) {
              cache_handle = block_cache->Insert(Slice(cache_key_buffer, 16),
                                                 block, block->size(),
                                                 &DeleteCachedBlock);
            }
          }
        }
      }
      if (block != nullptr) {
        s = block->SeekThen(rep_->options.comparator, k, arg, handle_result,
                            bytewise_internal);
        if (cache_handle != nullptr) {
          block_cache->Release(cache_handle);
        } else if (!mapped_block) {
          delete block;
        }
      }
    }
  }
  return s;
}

Status Table::InternalGetMany(const ReadOptions& options,
                              const std::vector<Slice>& keys,
                              std::vector<GetResult>* results,
                              size_t* data_block_probes) {
  results->clear();
  results->resize(keys.size());
  *data_block_probes = 0;
  Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
  Iterator* data_iter = nullptr;
  uint64_t current_block_offset = std::numeric_limits<uint64_t>::max();
  Status status;
  for (size_t i = 0; i < keys.size() && status.ok(); ++i) {
    const Slice& key = keys[i];
    index_iter->Seek(key);
    if (!index_iter->Valid()) {
      status = index_iter->status();
      continue;
    }

    Slice encoded_handle = index_iter->value();
    BlockHandle handle;
    status = handle.DecodeFrom(&encoded_handle);
    if (!status.ok()) break;
    FilterBlockReader* filter = rep_->filter;
    if (filter != nullptr && !filter->KeyMayMatch(handle.offset(), key)) {
      continue;
    }

    if (data_iter == nullptr || current_block_offset != handle.offset()) {
      delete data_iter;
      data_iter = BlockReader(this, options, index_iter->value());
      current_block_offset = handle.offset();
      ++*data_block_probes;
      status = data_iter->status();
      if (!status.ok()) break;
    }
    data_iter->Seek(key);
    if (data_iter->Valid() &&
        rep_->options.comparator->Compare(data_iter->key(), key) == 0) {
      GetResult& result = (*results)[i];
      result.key = data_iter->key().ToString();
      result.value = data_iter->value().ToString();
      result.found = true;
    }
    status = data_iter->status();
  }
  if (status.ok()) status = index_iter->status();
  delete data_iter;
  delete index_iter;
  return status;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
