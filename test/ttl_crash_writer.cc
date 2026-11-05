// A separate process for TTL crash recovery tests. Never closes the database:
// the parent waits for the sync acknowledgement, then sends SIGKILL.
#include <cstdio>
#include <cstring>
#include <atomic>
#include <memory>
#include <string>
#include <unistd.h>

#include "db/db_impl.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

namespace {
class ClockEnv : public leveldb::EnvWrapper {
 public:
  ClockEnv() : EnvWrapper(leveldb::Env::Default()) {}
  uint64_t NowMicros() override { return now_.load(); }
  void SetTime(uint64_t time) { now_.store(time); }
  void Arm(const char* mode) { mode_ = mode; armed_.store(true); }
  bool Match(const std::string& name, const char* mode, const char* suffix) {
    return armed_.load() && mode_ == mode && name.size() >= std::strlen(suffix) &&
        name.compare(name.size() - std::strlen(suffix), std::strlen(suffix), suffix) == 0;
  }
  void Stop() {
    if (!armed_.exchange(false)) return;
    if (::write(STDOUT_FILENO, "R", 1) != 1) _exit(7);
    for (;;) pause();
  }
  class File : public leveldb::WritableFile {
   public:
    File(ClockEnv* env, std::string name, leveldb::WritableFile* file)
        : env_(env), name_(std::move(name)), file_(file) {}
    leveldb::Status Append(const leveldb::Slice& data) override {
      leveldb::Status s = file_->Append(data);
      if (s.ok() && env_->Match(name_, "wal_partial", ".log")) {
        s = file_->Flush();
        if (s.ok()) env_->Stop();
      }
      return s;
    }
    leveldb::Status Close() override { return file_->Close(); }
    leveldb::Status Flush() override {
      leveldb::Status s = file_->Flush();
      if (s.ok() && env_->Match(name_, "wal_flushed", ".log")) env_->Stop();
      return s;
    }
    leveldb::Status Sync() override {
      if (armed_manifest("manifest_before_sync") &&
          name_.find("MANIFEST-") != std::string::npos) env_->Stop();
      leveldb::Status s = file_->Sync();
      if (s.ok() && (env_->Match(name_, "wal_synced", ".log") ||
                     env_->Match(name_, "sst_synced", ".ldb") ||
                     (armed_manifest("manifest_synced") && name_.find("MANIFEST-") != std::string::npos)))
        env_->Stop();
      return s;
    }
   private:
    bool armed_manifest(const char* mode) { return env_->armed_.load() && env_->mode_ == mode; }
    ClockEnv* env_;
    std::string name_;
    std::unique_ptr<leveldb::WritableFile> file_;
  };
  leveldb::Status NewWritableFile(const std::string& name, leveldb::WritableFile** out) override {
    leveldb::Status s = target()->NewWritableFile(name, out);
    if (s.ok()) *out = new File(this, name, *out);
    return s;
  }
  leveldb::Status NewAppendableFile(const std::string& name, leveldb::WritableFile** out) override {
    leveldb::Status s = target()->NewAppendableFile(name, out);
    if (s.ok()) *out = new File(this, name, *out);
    return s;
  }
 private:
  std::atomic<uint64_t> now_{1700000000000000ULL};
  std::string mode_;
  std::atomic<bool> armed_{false};
};
}

int main(int argc, char** argv) {
  if (argc != 3) return 2;
  ClockEnv env;
  leveldb::Options options;
  options.env = &env;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  leveldb::DB* db = nullptr;
  leveldb::Status status = leveldb::DB::Open(options, argv[1], &db);
  if (!status.ok()) return 3;
  if (std::strcmp(argv[2], "wal") != 0 && std::strcmp(argv[2], "sst") != 0) {
    leveldb::WriteOptions sync;
    sync.sync = true;
    if (!db->Put(sync, "stable", "before").ok()) return 8;
    const bool compact = std::strncmp(argv[2], "compact_", 8) == 0;
    const bool gc = std::strncmp(argv[2], "gc_", 3) == 0;
    if ((compact || gc) && !db->Put(sync, "first", "older").ok()) return 14;
    if ((compact || gc) &&
        !static_cast<leveldb::DBImpl*>(db)->TEST_CompactMemTable().ok()) return 12;
    leveldb::WriteBatch transaction;
    transaction.Put("first", std::string(1024, 'x'));
    transaction.PutWithExpiration("second", "ttl", env.NowMicros() + 2000000);
    if (compact || gc) {
      if (!db->Write(sync, &transaction).ok()) return 9;
      if (!static_cast<leveldb::DBImpl*>(db)->TEST_CompactMemTable().ok()) return 13;
      if (gc) env.SetTime(1700000003000000ULL);
      env.Arm(argv[2] + (compact ? 8 : 3));
      if (compact) db->CompactRange(nullptr, nullptr);
      else { leveldb::GCResult result; status = db->RunFullGC(&result); }
    } else if (std::strcmp(argv[2], "sst_synced") == 0 ||
        std::strcmp(argv[2], "manifest_synced") == 0 ||
        std::strcmp(argv[2], "manifest_before_sync") == 0) {
      if (!db->Write(sync, &transaction).ok()) return 9;
      env.Arm(argv[2]);
      status = static_cast<leveldb::DBImpl*>(db)->TEST_CompactMemTable();
    } else {
      env.Arm(argv[2]);
      leveldb::WriteOptions write;
      write.sync = std::strcmp(argv[2], "wal_synced") == 0;
      status = db->Write(write, &transaction);
    }
    return status.ok() ? 10 : 11;  // The requested hook must be reached.
  }
  leveldb::WriteBatch batch;
  batch.Put("plain", "permanent");
  batch.Put("victim", "old");
  batch.PutWithExpiration("victim", "ttl", env.NowMicros() + 2000000);
  batch.PutWithExpiration("zero", "expired", 0);
  batch.Put("gone", "value");
  batch.Delete("gone");
  leveldb::WriteOptions write;
  write.sync = true;
  status = db->Write(write, &batch);
  if (!status.ok()) return 4;
  if (std::strcmp(argv[2], "sst") == 0) {
    status = static_cast<leveldb::DBImpl*>(db)->TEST_CompactMemTable();
    if (!status.ok()) return 5;
  }
  if (::write(STDOUT_FILENO, "R", 1) != 1) return 6;
  for (;;) pause();
}
