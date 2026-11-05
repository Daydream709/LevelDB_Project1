#ifndef STORAGE_LEVELDB_TABLE_LOCAL_COUNT_INDEX_H_
#define STORAGE_LEVELDB_TABLE_LOCAL_COUNT_INDEX_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "leveldb/options.h"
#include "leveldb/status.h"

#include "table/block_builder.h"
#include "table/format.h"

namespace leveldb {

// Sorted raw keys enter this builder only after their complete per-file count
// has been aggregated. Each level retains one block, never a full directory.
class LocalCountIndexBuilder {
 public:
  using WriteBlock = std::function<Status(const Slice&, BlockHandle*)>;
  LocalCountIndexBuilder(size_t block_size, WriteBlock write);
  ~LocalCountIndexBuilder();
  Status Add(const Slice& key, uint64_t count);
  Status Finish(std::string* descriptor);

 private:
  struct Level;
  Status Flush(size_t level);
  Status Push(size_t level, const Slice& key, const Slice& value);
  Options options_;
  const size_t block_size_;
  WriteBlock write_;
  std::vector<std::unique_ptr<Level>> levels_;
  std::string previous_key_;
  uint64_t entries_ = 0;
  uint64_t records_ = 0;
  bool finished_ = false;
};

// Streams bytewise keys; custom-comparator inputs spill bounded sorted runs.
class LocalCountCollector {
 public:
  LocalCountCollector(const Options&, const std::string& temporary_prefix,
                      bool bytewise, LocalCountIndexBuilder*);
  ~LocalCountCollector();
  Status Add(const Slice&);
  Status Finish();

 private:
  Status Spill();
  Status Merge(const std::vector<std::string>&, const std::string&, bool final);
  Options options_;
  std::string prefix_;
  bool bytewise_;
  LocalCountIndexBuilder* index_;
  std::map<std::string, uint64_t> counts_;
  size_t bytes_ = 0;
  uint64_t next_run_ = 0;
  std::vector<std::string> runs_, owned_;
  std::vector<unsigned> run_levels_;
  std::string key_;
  uint64_t count_ = 0;
};

// Internal bridge: DB API and generic table builders remain unchanged.
class Table;
class TableBuilder;
class Cache;
class LocalCountTableAccess {
 public:
  static void Enable(TableBuilder*, const std::string& temporary_prefix);
  static Status Lookup(Table*, Cache*, const Slice&, uint64_t*);
};

// The reader supplies a validated, bounded-cache-backed block read. A failed
// optional-index lookup must be discarded in its entirety by the caller.
struct LocalCountBlock {
  std::string bytes;
  std::once_flag validate_once;
  Status validation;
  uint32_t validated_height = 0;
  std::string last_key;
};
using LocalCountReadBlock = std::function<Status(
    const BlockHandle&, std::shared_ptr<LocalCountBlock>*)>;
Status LookupLocalCount(const Slice& descriptor, const Slice& key,
                        uint64_t file_size, const LocalCountReadBlock& read,
                        uint64_t* count);

}  // namespace leveldb
#endif
