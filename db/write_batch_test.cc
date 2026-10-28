// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "gtest/gtest.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/logging.h"

#include <limits>

namespace leveldb {

class TTLRecordingHandler : public WriteBatch::Handler {
 public:
  void Put(const Slice&, const Slice&) override { ++plain_calls; }
  void Delete(const Slice&) override { ++delete_calls; }
  void PutWithTTL(const Slice& key, const Slice& value, uint64_t deadline) override {
    ++ttl_calls;
    last_key = key.ToString();
    last_value = value.ToString();
    last_deadline = deadline;
  }
  int plain_calls = 0;
  int delete_calls = 0;
  int ttl_calls = 0;
  std::string last_key, last_value;
  uint64_t last_deadline = 0;
};

TEST(WriteBatchTest, TTLDeadlineAndBinaryValueRoundTrip) {
  for (uint64_t deadline : {uint64_t{0}, std::numeric_limits<uint64_t>::max()}) {
    WriteBatch batch;
    const std::string key("k\0x", 3), value("v\0\xff", 3);
    batch.PutWithExpiration(key, value, deadline);
    TTLRecordingHandler handler;
    ASSERT_TRUE(batch.Iterate(&handler).ok());
    EXPECT_EQ(1, handler.ttl_calls);
    EXPECT_EQ(0, handler.plain_calls);
    EXPECT_EQ(0, handler.delete_calls);
    EXPECT_EQ(key, handler.last_key);
    EXPECT_EQ(value, handler.last_value);
    EXPECT_EQ(deadline, handler.last_deadline);
  }
}

TEST(WriteBatchTest, TTLTruncationsRejectIncompleteDeadlineAndValue) {
  WriteBatch batch;
  batch.PutWithExpiration("key", "payload", 123456789);
  const std::string encoded = WriteBatchInternal::Contents(&batch).ToString();
  for (size_t size = 12; size < encoded.size(); ++size) {
    SCOPED_TRACE(size);
    WriteBatch truncated;
    WriteBatchInternal::SetContents(&truncated, Slice(encoded.data(), size));
    TTLRecordingHandler handler;
    EXPECT_TRUE(truncated.Iterate(&handler).IsCorruption());
    EXPECT_EQ(0, handler.ttl_calls);
    EXPECT_EQ(0, handler.plain_calls);
    EXPECT_EQ(0, handler.delete_calls);
  }
  std::string bad_tag = encoded;
  bad_tag[12] = static_cast<char>(0xff);
  WriteBatch invalid;
  WriteBatchInternal::SetContents(&invalid, bad_tag);
  TTLRecordingHandler handler;
  EXPECT_TRUE(invalid.Iterate(&handler).IsCorruption());
  EXPECT_EQ(0, handler.ttl_calls);
}

static std::string PrintContents(WriteBatch* b) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* mem = new MemTable(cmp);
  mem->Ref();
  std::string state;
  Status s = WriteBatchInternal::InsertInto(b, mem);
  int count = 0;
  Iterator* iter = mem->NewIterator();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    EXPECT_TRUE(ParseInternalKey(iter->key(), &ikey));
    switch (ikey.type) {
      case kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
    }
    state.append("@");
    state.append(NumberToString(ikey.sequence));
  }
  delete iter;
  if (!s.ok()) {
    state.append("ParseError()");
  } else if (count != WriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  mem->Unref();
  return state;
}

TEST(WriteBatchTest, Empty) {
  WriteBatch batch;
  ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0, WriteBatchInternal::Count(&batch));
}

TEST(WriteBatchTest, Multiple) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  batch.Put(Slice("baz"), Slice("boo"));
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_EQ(100, WriteBatchInternal::Sequence(&batch));
  ASSERT_EQ(3, WriteBatchInternal::Count(&batch));
  ASSERT_EQ(
      "Put(baz, boo)@102"
      "Delete(box)@101"
      "Put(foo, bar)@100",
      PrintContents(&batch));
}

TEST(WriteBatchTest, Corruption) {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  WriteBatchInternal::SetSequence(&batch, 200);
  Slice contents = WriteBatchInternal::Contents(&batch);
  WriteBatchInternal::SetContents(&batch,
                                  Slice(contents.data(), contents.size() - 1));
  ASSERT_EQ(
      "Put(foo, bar)@200"
      "ParseError()",
      PrintContents(&batch));
}

TEST(WriteBatchTest, Append) {
  WriteBatch b1, b2;
  WriteBatchInternal::SetSequence(&b1, 200);
  WriteBatchInternal::SetSequence(&b2, 300);
  b1.Append(b2);
  ASSERT_EQ("", PrintContents(&b1));
  b2.Put("a", "va");
  b1.Append(b2);
  ASSERT_EQ("Put(a, va)@200", PrintContents(&b1));
  b2.Clear();
  b2.Put("b", "vb");
  b1.Append(b2);
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@201",
      PrintContents(&b1));
  b2.Delete("foo");
  b1.Append(b2);
  ASSERT_EQ(
      "Put(a, va)@200"
      "Put(b, vb)@202"
      "Put(b, vb)@201"
      "Delete(foo)@203",
      PrintContents(&b1));
}

TEST(WriteBatchTest, ApproximateSize) {
  WriteBatch batch;
  size_t empty_size = batch.ApproximateSize();

  batch.Put(Slice("foo"), Slice("bar"));
  size_t one_key_size = batch.ApproximateSize();
  ASSERT_LT(empty_size, one_key_size);

  batch.Put(Slice("baz"), Slice("boo"));
  size_t two_keys_size = batch.ApproximateSize();
  ASSERT_LT(one_key_size, two_keys_size);

  batch.Delete(Slice("box"));
  size_t post_delete_size = batch.ApproximateSize();
  ASSERT_LT(two_keys_size, post_delete_size);
}

}  // namespace leveldb
