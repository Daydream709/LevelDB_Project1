// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "util/profiler.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), num_entries_(0),
      table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // The official write benchmark and many applications use 16-byte keys.
  // Their internal key length is 24 and fits in one varint byte.
  if (bytewise_comparator &&
      static_cast<unsigned char>(aptr[0]) == 24 &&
      static_cast<unsigned char>(bptr[0]) == 24) {
    const char* a_key = aptr + 1;
    const char* b_key = bptr + 1;
#if defined(__GNUC__) || defined(__clang__)
    // DecodeFixed64 is unaligned-safe. Reversing its little-endian numeric
    // result makes unsigned comparisons follow lexicographic byte order.
    const uint64_t a_first = __builtin_bswap64(DecodeFixed64(a_key));
    const uint64_t b_first = __builtin_bswap64(DecodeFixed64(b_key));
    if (a_first != b_first) return a_first < b_first ? -1 : 1;
    const uint64_t a_second = __builtin_bswap64(DecodeFixed64(a_key + 8));
    const uint64_t b_second = __builtin_bswap64(DecodeFixed64(b_key + 8));
    if (a_second != b_second) return a_second < b_second ? -1 : 1;
#else
    const int user_order = std::memcmp(a_key, b_key, 16);
    if (user_order != 0) return user_order;
#endif
    const uint64_t a_tag = DecodeFixed64(a_key + 16);
    const uint64_t b_tag = DecodeFixed64(b_key + 16);
    if (a_tag > b_tag) return -1;
    if (a_tag < b_tag) return 1;
    return 0;
  }
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
  ++num_entries_;
  if (num_entries_ == 1) has_entries_.store(true, std::memory_order_release);
}

uint64_t MemTable::CountUserKeyRecords(const Slice& key) const {
  uint64_t result = 0;
  InternalKey seek(key, kMaxSequenceNumber, kValueTypeForSeek);
  Iterator* iter = const_cast<MemTable*>(this)->NewIterator();
  iter->Seek(seek.Encode());
  const Comparator* user_comparator =
      comparator_.comparator.user_comparator();
  while (iter->Valid()) {
    ParsedInternalKey parsed;
    if (!ParseInternalKey(iter->key(), &parsed) ||
        user_comparator->Compare(parsed.user_key, key) != 0) {
      break;
    }
    if (parsed.user_key == key) {
      if (result == std::numeric_limits<uint64_t>::max()) break;
      ++result;
    }
    iter->Next();
  }
  delete iter;
  return result;
}

bool MemTable::Get(const LookupKey& key, uint64_t read_time_micros,
                   std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeValueWithTTL: {
          ProfileScope ttl_check("ttl.check");
          ProfileScope::Count("ttl.values_examined");
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          if (v.size() < 8) {
            *s = Status::Corruption("TTL value is shorter than its deadline");
          } else {
            const uint64_t expire_time_micros = DecodeFixed64(v.data());
            if (read_time_micros >= expire_time_micros) {
              ProfileScope::Count("ttl.expired_read_results");
              *s = Status::NotFound(Slice());
            } else {
              value->assign(v.data() + 8, v.size() - 8);
            }
          }
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
