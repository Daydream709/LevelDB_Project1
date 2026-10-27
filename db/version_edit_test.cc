// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "gtest/gtest.h"

namespace leveldb {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig + 400 + i,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.RemoveFile(4, kBig + 700 + i);
    edit.SetCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  MetadataFileMeta metadata_file;
  metadata_file.kind = kMetadataChange;
  metadata_file.number = kBig + 1100;
  metadata_file.file_size = kBig + 1200;
  metadata_file.min_mseq = kBig + 1300;
  metadata_file.max_mseq = kBig + 1300;
  metadata_file.level = 1;
  metadata_file.smallest = "a";
  metadata_file.largest = "z";
  edit.AddMetadataFile(metadata_file);
  edit.RemoveMetadataFile(kBig + 1400);
  MetadataCommitMeta metadata_commit;
  metadata_commit.file_number = metadata_file.number;
  metadata_commit.prev_mseq = kBig + 1500;
  metadata_commit.mseq = kBig + 1501;
  metadata_commit.total_entries = kBig + 1600;
  metadata_commit.distinct_keys = kBig + 1700;
  metadata_commit.wal_number = kBig + 1800;
  edit.AddMetadataCommit(metadata_commit);
  edit.SetMetadataCheckpoint(kBig + 1200, kBig + 2100, kBig + 2000,
                             kBig + 2200);
  TestEncodeDecode(edit);
}

}  // namespace leveldb
