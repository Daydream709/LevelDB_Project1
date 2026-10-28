// A separate process for TTL crash recovery tests. Never closes the database:
// the parent waits for the sync acknowledgement, then sends SIGKILL.
#include <cstdio>
#include <cstring>
#include <unistd.h>

#include "db/db_impl.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

namespace {
class ClockEnv : public leveldb::EnvWrapper {
 public:
  ClockEnv() : EnvWrapper(leveldb::Env::Default()) {}
  uint64_t NowMicros() override { return 1700000000000000ULL; }
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
