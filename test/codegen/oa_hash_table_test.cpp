//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_test.cpp
//
// Identification: test/codegen/oa_hash_table_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>

#include "common/harness.h"

#include "murmur3/MurmurHash3.h"

#include "codegen/util/oa_hash_table.h"
#include "common/timer.h"

namespace peloton {
namespace test {

class OAHashTableTest : public PelotonTest {
 public:
  // The key and value object we store in the hash table
  struct Key {
    uint32_t k1, k2;

    bool operator==(const Key &rhs) const {
      return k1 == rhs.k1 && k2 == rhs.k2;
    }
    bool operator!=(const Key &rhs) const { return !(rhs == *this); }
  };

  struct Value {
    uint32_t v1, v2, v3, v4;
    bool operator==(const Value &rhs) const {
      return v1 == rhs.v1 && v2 == rhs.v2 && v3 == rhs.v3 && v4 == rhs.v4;
    }
    bool operator!=(const Value &rhs) const { return !(rhs == *this); }
  };

  OAHashTableTest() {
    PL_MEMSET(raw_hash_table, 1, sizeof(raw_hash_table));
    GetHashTable().Init(sizeof(Key), sizeof(Value));
  }

  ~OAHashTableTest() {
    // Clean up
    GetHashTable().Destroy();
  }

  static inline uint32_t Hash(const Key &k) {
    static constexpr uint32_t seed = 12345;
    auto h1 = MurmurHash3_x86_32(&k.k1, sizeof(uint32_t), seed);
    auto h2 = MurmurHash3_x86_32(&k.k2, sizeof(uint32_t), seed);
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }

  inline void Insert(Key k, Value v) {
    auto &hash_table = GetHashTable();
    hash_table.Insert(Hash(k), k, v);
  }

  inline void Reset() {
    GetHashTable().Destroy();
    GetHashTable().Init(sizeof(Key), sizeof(Value));
  }

  codegen::util::OAHashTable &GetHashTable() {
    return *reinterpret_cast<codegen::util::OAHashTable *>(raw_hash_table);
  }

 private:
  // The open-addressing hash-table instance
  int8_t raw_hash_table[sizeof(codegen::util::OAHashTable)];
};

TEST_F(OAHashTableTest, CanInsertKeyValuePairs) {
  Value v = {3, 4, 5, 6};

  uint32_t to_insert = 50000;

  // Insert a bunch of unique keys
  for (uint32_t i = 0; i < to_insert; i++) {
    Insert({1, i}, v);
  }

  // Check validity
  EXPECT_EQ(to_insert, GetHashTable().NumEntries());
  EXPECT_EQ(to_insert, GetHashTable().NumOccupiedBuckets());

  // Insert a duplicate key-value pair
  Insert({1, 0}, v);

  // Duplicate keys don't occupy additional buckets
  EXPECT_EQ(to_insert + 1, GetHashTable().NumEntries());
  EXPECT_EQ(to_insert, GetHashTable().NumOccupiedBuckets());
}

TEST_F(OAHashTableTest, CanIterate) {
  Value v = {3, 4, 5, 6};

  uint32_t to_insert = 50000;

  // Insert a bunch of unique keys
  for (uint32_t i = 0; i < to_insert; i++) {
    Insert({1, i}, v);
  }

  auto &hashtable = GetHashTable();

  // Check that we find them all
  uint32_t i = 0;
  for (auto iter = hashtable.begin(), end = hashtable.end(); iter != end;
       ++iter) {
    i++;
  }

  EXPECT_EQ(to_insert, i);

  // Insert two duplicate keys to make sure iteration catches it
  Key key_dup{1, 0};
  Value vdup1 = {6, 5, 4, 3}, vdup2{4, 4, 4, 4};

  Insert(key_dup, vdup1);
  Insert(key_dup, vdup2);

  i = 0;
  uint32_t dup_count = 0;
  for (auto iter = hashtable.begin(), end = hashtable.end();
       iter != end; ++iter) {
    const Key *iter_key = reinterpret_cast<const Key *>(iter.Key());
    if (*iter_key == key_dup) {
      dup_count++;
      const Value *iter_val = reinterpret_cast<const Value *>(iter.Value());
      EXPECT_TRUE(*iter_val == v || *iter_val == vdup1 || *iter_val == vdup2);
    }
    i++;
  }

  EXPECT_EQ(to_insert + 2, i);
  EXPECT_EQ(3, dup_count);
}

TEST_F(OAHashTableTest, CanCodegenProbeOrInsert) {}

TEST_F(OAHashTableTest, MicroBenchmark) {
  uint32_t num_runs = 10;

  std::vector<Key> keys;
  Value v = {6, 5, 4, 3};

  // Create all keys
  uint32_t num_keys = 100000;
  for (uint32_t i = 0; i < num_keys; i++) {
    keys.push_back({1, static_cast<uint32_t>(rand())});
  }

  double avg_oaht = 0.0;
  double avg_map = 0.0;

  // First, bench ours ...
  {
    for (uint32_t b = 0; b < num_runs; b++) {
      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      // Start
      for (uint32_t i = 0; i < num_keys; i++) {
        Insert(keys[i], v);
      }
      // End

      timer.Stop();
      avg_oaht += timer.GetDuration();

      // Reset
      Reset();
    }
  }

  // Next, unordered_map ...
  {
    struct Hasher {
      size_t operator()(const Key &k) const { return Hash(k); }
    };

    for (uint32_t b = 0; b < num_runs; b++) {
      std::unordered_map<Key, Value, Hasher> ht{
          codegen::util::OAHashTable::kDefaultInitialSize};

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      for (uint32_t i = 0; i < num_keys; i++) {
        ht.insert(std::make_pair(keys[i], v));
      }

      timer.Stop();
      avg_map += timer.GetDuration();
    }
  }

  LOG_INFO("Avg OA_HT: %.2lf, Avg std::unordered_map: %.2lf",
          avg_oaht / (double)num_runs, avg_map / (double)num_runs);
}

}  // namespace test
}  // namespace peloton
