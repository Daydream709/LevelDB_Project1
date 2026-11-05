#include "table/local_count_index.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/profiler.h"

namespace leveldb {
namespace {
constexpr size_t kMaxLevels = 32;
Status InvalidIndex() {
  return Status::Corruption("invalid optional local count index");
}
bool ValidHandle(const BlockHandle& handle, uint64_t size) {
  return size >= Footer::kEncodedLength + kBlockTrailerSize &&
         handle.offset() <= size - Footer::kEncodedLength - kBlockTrailerSize &&
         handle.size() <= size - Footer::kEncodedLength - kBlockTrailerSize -
                              handle.offset();
}
}  // namespace

struct LocalCountIndexBuilder::Level {
  explicit Level(const Options* options) : block(options) {}
  BlockBuilder block;
  std::string last;
  uint64_t entries = 0;
};

LocalCountIndexBuilder::LocalCountIndexBuilder(size_t size, WriteBlock write)
    : block_size_(size < 1024 ? 1024 : size), write_(std::move(write)) {
  options_.comparator = BytewiseComparator();
  options_.block_restart_interval = 16;
}
LocalCountIndexBuilder::~LocalCountIndexBuilder() = default;

Status LocalCountIndexBuilder::Push(size_t level, const Slice& key,
                                    const Slice& value) {
  if (level >= kMaxLevels) return InvalidIndex();
  while (levels_.size() <= level) levels_.emplace_back(new Level(&options_));
  Level* current = levels_[level].get();
  // Flush before adding, so a single exceptionally long key cannot create
  // an unbounded parent fanout or repeatedly flush a one-child directory.
  if (current->entries >= 2 &&
      current->block.CurrentSizeEstimate() >= block_size_) {
    Status s = Flush(level);
    if (!s.ok()) return s;
  }
  if (current->entries && Slice(current->last).compare(key) >= 0)
    return InvalidIndex();
  current->block.Add(key, value);
  current->last.assign(key.data(), key.size());
  ++current->entries;
  return Status::OK();
}

Status LocalCountIndexBuilder::Flush(size_t level) {
  Level* current = levels_[level].get();
  if (current->entries == 0) return Status::OK();
  BlockHandle handle;
  Status s = write_(current->block.Finish(), &handle);
  if (!s.ok()) return s;
  std::string encoded;
  handle.EncodeTo(&encoded);
  s = Push(level + 1, current->last, encoded);
  current->block.Reset();
  current->entries = 0;
  current->last.clear();
  return s;
}

Status LocalCountIndexBuilder::Add(const Slice& key, uint64_t count) {
  if (finished_ || count == 0 ||
      (entries_ != 0 && Slice(previous_key_).compare(key) >= 0) ||
      entries_ == std::numeric_limits<uint64_t>::max() ||
      records_ > std::numeric_limits<uint64_t>::max() - count)
    return InvalidIndex();
  char value[8];
  EncodeFixed64(value, count);
  Status s = Push(0, key, Slice(value, sizeof(value)));
  if (s.ok()) {
    previous_key_.assign(key.data(), key.size());
    ++entries_;
    records_ += count;
  }
  return s;
}

Status LocalCountIndexBuilder::Finish(std::string* descriptor) {
  if (finished_ || descriptor == nullptr) return InvalidIndex();
  finished_ = true;
  descriptor->clear();
  if (entries_ == 0) return Status::OK();
  for (size_t level = 0; level < levels_.size(); ++level) {
    if (level + 1 < levels_.size()) {
      Status s = Flush(level);
      if (!s.ok()) return s;
      continue;
    }
    BlockHandle root;
    Status s = write_(levels_[level]->block.Finish(), &root);
    if (!s.ok()) return s;
    descriptor->append("LCI1", 4);
    PutVarint32(descriptor, static_cast<uint32_t>(level));
    PutVarint64(descriptor, entries_);
    PutVarint64(descriptor, records_);
    root.EncodeTo(descriptor);
    return Status::OK();
  }
  return InvalidIndex();
}

Status LookupLocalCount(const Slice& descriptor, const Slice& key,
                        uint64_t file_size, const LocalCountReadBlock& read,
                        uint64_t* count) {
  if (count == nullptr) return Status::InvalidArgument("null local count");
  *count = 0;
  Slice input = descriptor;
  if (input.size() < 4 || Slice(input.data(), 4) != Slice("LCI1"))
    return InvalidIndex();
  input.remove_prefix(4);
  uint32_t height;
  uint64_t entries, records;
  BlockHandle handle;
  if (!GetVarint32(&input, &height) || height >= kMaxLevels ||
      !GetVarint64(&input, &entries) || entries == 0 ||
      !GetVarint64(&input, &records) || records < entries ||
      !handle.DecodeFrom(&input).ok() || !input.empty())
    return InvalidIndex();
  std::string expected_bound;
  bool has_bound = false;
  for (;;) {
    if (!ValidHandle(handle, file_size)) return InvalidIndex();
    const uint64_t parent_offset = handle.offset();
    std::shared_ptr<LocalCountBlock> decoded;
    Status s = read(handle, &decoded);
    if (!s.ok()) return s;
    BlockContents contents;
    contents.data = Slice(decoded->bytes);
    contents.cachable = false;
    contents.heap_allocated = false;
    Block block(contents);
    std::unique_ptr<Iterator> iter(block.NewIterator(BytewiseComparator()));
    std::call_once(decoded->validate_once, [&] {
      decoded->validated_height = height;
      decoded->validation = [&]() -> Status {
        std::string previous;
        bool first = true;
        uint64_t sum = 0, block_entries = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
          if (!first && Slice(previous).compare(iter->key()) >= 0)
            return InvalidIndex();
          first = false;
          ++block_entries;
          previous.assign(iter->key().data(), iter->key().size());
          if (height == 0) {
            if (iter->value().size() != 8) return InvalidIndex();
            uint64_t value = DecodeFixed64(iter->value().data());
            if (value == 0 || sum > records || value > records - sum)
              return InvalidIndex();
            sum += value;
          } else {
            Slice value = iter->value();
            BlockHandle child;
            if (!child.DecodeFrom(&value).ok() || !value.empty() ||
                !ValidHandle(child, file_size) ||
                child.offset() >= parent_offset ||
                child.size() + kBlockTrailerSize >
                    parent_offset - child.offset())
              return InvalidIndex();
          }
        }
        if (!iter->status().ok()) return iter->status();
        if (first || (!has_bound && height == 0 &&
                      (sum != records || block_entries != entries)))
          return InvalidIndex();
        decoded->last_key = previous;
        return Status::OK();
      }();
    });
    if (!decoded->validation.ok()) return decoded->validation;
    if (decoded->validated_height != height ||
        (has_bound && decoded->last_key != expected_bound))
      return InvalidIndex();
    iter->Seek(key);
    if (!iter->status().ok()) return iter->status();
    if (!iter->Valid()) return Status::OK();
    if (height == 0) {
      if (iter->key() == key) *count = DecodeFixed64(iter->value().data());
      return Status::OK();
    }
    expected_bound = iter->key().ToString();
    has_bound = true;
    input = iter->value();
    if (!handle.DecodeFrom(&input).ok() || !input.empty())
      return InvalidIndex();
    --height;
  }
}

LocalCountCollector::LocalCountCollector(const Options& options,
                                         const std::string& prefix,
                                         bool bytewise,
                                         LocalCountIndexBuilder* index)
    : options_(options), prefix_(prefix), bytewise_(bytewise), index_(index) {
  options_.comparator = BytewiseComparator();
  options_.filter_policy = nullptr;
  options_.block_cache = nullptr;
}
LocalCountCollector::~LocalCountCollector() {
  for (const auto& file : owned_) options_.env->RemoveFile(file);
}
Status LocalCountCollector::Add(const Slice& key) {
  if (bytewise_) {
    if (count_ != 0 && key != Slice(key_)) {
      Status s = index_->Add(key_, count_);
      if (!s.ok()) return s;
      count_ = 0;
    }
    if (count_ == 0) key_ = key.ToString();
    if (count_ == UINT64_MAX) return InvalidIndex();
    ++count_;
    return Status::OK();
  }
  auto inserted = counts_.emplace(key.ToString(), 0);
  if (inserted.second)
    bytes_ += key.size() + sizeof(std::pair<const std::string, uint64_t>) +
              4 * sizeof(void*);
  if (inserted.first->second == UINT64_MAX) return InvalidIndex();
  ++inserted.first->second;
  return bytes_ >= options_.metadata_workspace_size ? Spill() : Status::OK();
}
namespace {
Status AppendCountRun(WritableFile* file, const Slice& key, uint64_t count) {
  if (key.size() > UINT32_MAX || count == 0) return InvalidIndex();
  std::string record;
  PutFixed32(&record, static_cast<uint32_t>(key.size()));
  record.append(key.data(), key.size());
  PutFixed64(&record, count);
  PutFixed32(&record, crc32c::Value(record.data(), record.size()));
  return file->Append(record);
}
struct CountRunInput {
  std::unique_ptr<SequentialFile> file;
  std::string key, previous;
  uint64_t count = 0, remaining = 0;
  bool valid = false, started = false;
  Status ReadExact(size_t size, std::string* output, bool eof_allowed = false) {
    if (eof_allowed && remaining == 0) {
      output->clear();
      return Status::OK();
    }
    if (size > remaining)
      return Status::Corruption("local count run length exceeds file");
    output->resize(size);
    Slice data;
    Status s = file->Read(size, &data, &(*output)[0]);
    if (!s.ok()) return s;
    if (data.empty() && eof_allowed) {
      output->clear();
      return s;
    }
    if (data.size() != size)
      return Status::Corruption("truncated local count run");
    remaining -= data.size();
    if (data.data() != output->data()) output->assign(data.data(), data.size());
    return s;
  }
  Status Next() {
    valid = false;
    std::string header, tail;
    Status s = ReadExact(4, &header, true);
    if (!s.ok() || header.empty()) return s;
    const uint32_t length = DecodeFixed32(header.data());
    s = ReadExact(static_cast<size_t>(length) + 12, &tail);
    if (!s.ok()) return s;
    uint32_t crc = crc32c::Extend(crc32c::Value(header.data(), 4), tail.data(),
                                  length + 8);
    if (crc != DecodeFixed32(tail.data() + length + 8))
      return Status::Corruption("local count run checksum mismatch");
    key.assign(tail.data(), length);
    count = DecodeFixed64(tail.data() + length);
    if (count == 0 || (started && Slice(previous).compare(key) >= 0))
      return InvalidIndex();
    previous = key;
    started = valid = true;
    return Status::OK();
  }
};
}  // namespace
Status LocalCountCollector::Spill() {
  if (counts_.empty()) return Status::OK();
  ProfileScope::Count("metadata.local_count_spill_runs");
  const std::string path =
      prefix_ + ".counts." + std::to_string(next_run_++) + ".dbtmp";
  owned_.push_back(path);
  WritableFile* file = nullptr;
  Status s = options_.env->NewWritableFile(path, &file);
  if (!s.ok()) return s;
  for (const auto& item : counts_) {
    s = AppendCountRun(file, item.first, item.second);
    if (!s.ok()) break;
  }
  Status close = file->Close();
  delete file;
  if (s.ok()) s = close;
  if (!s.ok()) return s;
  runs_.push_back(path);
  run_levels_.push_back(0);
  counts_.clear();
  bytes_ = 0;
  // Online 8-way carries bound both live run descriptors and merge fan-in.
  while (runs_.size() >= 8) {
    const size_t start = runs_.size() - 8;
    const unsigned level = run_levels_.back();
    bool same = true;
    for (size_t i = start; i < runs_.size(); ++i)
      same &= run_levels_[i] == level;
    if (!same) break;
    const std::string merged =
        prefix_ + ".counts." + std::to_string(next_run_++) + ".dbtmp";
    owned_.push_back(merged);
    std::vector<std::string> group(runs_.begin() + start, runs_.end());
    s = Merge(group, merged, false);
    if (!s.ok()) return s;
    for (const auto& old : group) {
      options_.env->RemoveFile(old);
      owned_.erase(std::remove(owned_.begin(), owned_.end(), old),
                   owned_.end());
    }
    runs_.resize(start);
    run_levels_.resize(start);
    runs_.push_back(merged);
    run_levels_.push_back(level + 1);
  }
  return Status::OK();
}
Status LocalCountCollector::Merge(const std::vector<std::string>& paths,
                                  const std::string& output, bool final) {
  ProfileScope::Count("metadata.local_count_merge_runs", paths.size());
  std::vector<CountRunInput> inputs(paths.size());
  for (size_t i = 0; i < paths.size(); ++i) {
    SequentialFile* file = nullptr;
    Status s = options_.env->NewSequentialFile(paths[i], &file);
    inputs[i].file.reset(file);
    if (s.ok()) s = options_.env->GetFileSize(paths[i], &inputs[i].remaining);
    if (s.ok()) s = inputs[i].Next();
    if (!s.ok()) return s;
  }
  std::unique_ptr<WritableFile> file;
  if (!final) {
    WritableFile* f = nullptr;
    Status s = options_.env->NewWritableFile(output, &f);
    file.reset(f);
    if (!s.ok()) return s;
  }
  Status status;
  while (status.ok()) {
    CountRunInput* first = nullptr;
    for (auto& input : inputs) {
      if (input.valid && (!first || Slice(input.key).compare(first->key) < 0))
        first = &input;
    }
    if (!first) break;
    const std::string key = first->key;
    uint64_t count = 0;
    for (auto& input : inputs) {
      if (input.valid && input.key == key) {
        if (count > UINT64_MAX - input.count) {
          status = InvalidIndex();
          break;
        }
        count += input.count;
        status = input.Next();
        if (!status.ok()) break;
      }
    }
    if (!status.ok()) break;
    status = final ? index_->Add(key, count)
                   : AppendCountRun(file.get(), key, count);
  }
  if (file) {
    Status close = file->Close();
    if (status.ok()) status = close;
  }
  return status;
}
Status LocalCountCollector::Finish() {
  if (bytewise_) return count_ == 0 ? Status::OK() : index_->Add(key_, count_);
  if (runs_.empty()) {
    for (const auto& item : counts_) {
      Status s = index_->Add(item.first, item.second);
      if (!s.ok()) return s;
    }
    return Status::OK();
  }
  Status s = Spill();
  if (!s.ok()) return s;
  while (runs_.size() > 8) {
    std::vector<std::string> next;
    for (size_t i = 0; i < runs_.size(); i += 8) {
      const std::string path =
          prefix_ + ".counts." + std::to_string(next_run_++) + ".dbtmp";
      owned_.push_back(path);
      std::vector<std::string> group(
          runs_.begin() + i, runs_.begin() + std::min(runs_.size(), i + 8));
      s = Merge(group, path, false);
      if (!s.ok()) return s;
      for (const auto& old : group) {
        options_.env->RemoveFile(old);
        owned_.erase(std::remove(owned_.begin(), owned_.end(), old),
                     owned_.end());
      }
      next.push_back(path);
    }
    runs_.swap(next);
  }
  return Merge(runs_, "", true);
}

}  // namespace leveldb
