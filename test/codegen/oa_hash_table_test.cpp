//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_test.cpp
//
// Identification: test/codegen/oa_hash_table_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>

#include "common/harness.h"

#include "murmur3/MurmurHash3.h"
#include "libcuckoo/cuckoohash_map.hh"

#include "codegen/util/oa_hash_table.h"
#include "common/timer.h"

namespace peloton {
namespace test {

class OAHashTableTest : public PelotonTest {
 public:
  // The key and value object we store in the hash table
  struct Key {
    uint32_t k1, k2;

    Key(uint32_t _k1, uint32_t _k2) : k1(_k1), k2(_k2) {}

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

  OAHashTableTest() : ht_(sizeof(Key), sizeof(Value)) {}

  static uint32_t Hash(const Key &k) {
    static constexpr uint32_t seed = 12345;
    auto h1 = MurmurHash3_x86_32(&k.k1, sizeof(uint32_t), seed);
    auto h2 = MurmurHash3_x86_32(&k.k2, sizeof(uint32_t), seed);
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }

  void Insert(Key k, Value v) { GetHashTable().Insert(Hash(k), k, v); }

  codegen::util::OAHashTable &GetHashTable() { return ht_; }

 private:
  // The open-addressing hash-table instance
  codegen::util::OAHashTable ht_;
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
  for (auto iter = hashtable.begin(), end = hashtable.end(); iter != end;
       ++iter) {
    const auto *iter_key = reinterpret_cast<const Key *>(iter.Key());
    if (*iter_key == key_dup) {
      dup_count++;
      const auto *iter_val = reinterpret_cast<const Value *>(iter.Value());
      EXPECT_TRUE(*iter_val == v || *iter_val == vdup1 || *iter_val == vdup2);
    }
    i++;
  }

  EXPECT_EQ(to_insert + 2, i);
  EXPECT_EQ(3, dup_count);
}

TEST_F(OAHashTableTest, MicroBenchmark) {
  uint32_t num_runs = 10;

  std::vector<Key> keys;
  Value v = {6, 5, 4, 3};

  std::random_device r;
  std::default_random_engine e(r());
  std::uniform_int_distribution<uint32_t> gen;

  // Create all keys
  uint32_t num_keys = 100000;
  for (uint32_t i = 0; i < num_keys; i++) {
    keys.emplace_back(gen(e), gen(e));
  }

  double avg_oaht_insert = 0.0, avg_oaht_probe = 0.0;
  double avg_map_insert = 0.0, avg_map_probe = 0.0;
  double avg_cuckoo_insert = 0.0, avg_cuckoo_probe = 0.0;

  // First, bench ours ...
  {
    for (uint32_t b = 0; b < num_runs; b++) {
      codegen::util::OAHashTable ht(
          sizeof(Key), sizeof(Value),
          codegen::util::OAHashTable::kDefaultInitialSize);

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      // Start Insert
      for (uint32_t i = 0; i < num_keys; i++) {
        ht.Insert(Hash(keys[i]), keys[i], v);
      }
      // End Insert

      timer.Stop();
      avg_oaht_insert += timer.GetDuration();

      timer.Reset();
      timer.Start();

      // Start Probe
      std::vector<Key> shuffled = keys;
      std::random_shuffle(shuffled.begin(), shuffled.end());
      for (uint32_t i = 0; i < num_keys; i++) {
        Value probe_val;
        EXPECT_TRUE(ht.Probe(Hash(shuffled[i]), shuffled[i], probe_val));
      }
      // End Probe

      timer.Stop();
      avg_oaht_probe += timer.GetDuration();
    }
  }

  // Next, unordered_map ...
  {
    struct Hasher {
      size_t operator()(const Key &k) const { return Hash(k); }
    };

    for (uint32_t b = 0; b < num_runs; b++) {
      std::unordered_map<Key, Value, Hasher> ht(
          codegen::util::OAHashTable::kDefaultInitialSize);

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      for (uint32_t i = 0; i < num_keys; i++) {
        ht.insert(std::make_pair(keys[i], v));
      }

      timer.Stop();
      avg_map_insert += timer.GetDuration();

      timer.Reset();
      timer.Start();

      // Start Probe
      std::vector<Key> shuffled = keys;
      std::random_shuffle(shuffled.begin(), shuffled.end());
      for (uint32_t i = 0; i < num_keys; i++) {
        EXPECT_NE(ht.find(shuffled[i]), ht.end());
      }
      // End Probe

      timer.Stop();
      avg_map_probe += timer.GetDuration();
    }
  }

  // Next, cuckoo map
  {
    struct Hasher {
      size_t operator()(const Key &k) const { return Hash(k); }
    };

    for (uint32_t b = 0; b < num_runs; b++) {
      cuckoohash_map<Key, Value, Hasher> map(
          codegen::util::OAHashTable::kDefaultInitialSize);

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();

      for (uint32_t i = 0; i < num_keys; i++) {
        map.insert(keys[i], v);
      }

      timer.Stop();
      avg_cuckoo_insert += timer.GetDuration();

      timer.Reset();
      timer.Start();

      // Start Probe
      std::vector<Key> shuffled = keys;
      std::random_shuffle(shuffled.begin(), shuffled.end());
      for (uint32_t i = 0; i < num_keys; i++) {
        Value probe_val;
        EXPECT_TRUE(map.find(shuffled[i], probe_val));
      }
      // End Probe

      timer.Stop();
      avg_cuckoo_probe += timer.GetDuration();
    }
  }

  LOG_INFO("OA_HT insert: %.2lf, probe: %.2lf",
           avg_oaht_insert / (double)num_runs,
           avg_oaht_probe / (double)num_runs);
  LOG_INFO("std::unordered_map insert: %.2lf, probe: %.2lf",
           avg_map_insert / (double)num_runs, avg_map_probe / (double)num_runs);
  LOG_INFO("Cuckoo insert: %.2lf, probe: %.2lf",
           avg_cuckoo_insert / (double)num_runs,
           avg_cuckoo_probe / (double)num_runs);
}

}  // namespace test
}  // namespace peloton
