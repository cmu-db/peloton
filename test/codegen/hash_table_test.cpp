//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table_test.cpp
//
// Identification: test/codegen/hash_table_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <iosfwd>
#include <random>

#include "murmur3/MurmurHash3.h"

#include "common/harness.h"
#include "common/timer.h"
#include "codegen/util/hash_table.h"

namespace peloton {
namespace test {

/**
 * The test key used in the hash table
 */
struct Key {
  uint32_t k1, k2;

  Key() = default;
  Key(uint32_t _k1, uint32_t _k2) : k1(_k1), k2(_k2) {}

  bool operator==(const Key &rhs) const { return k1 == rhs.k1 && k2 == rhs.k2; }
  bool operator!=(const Key &rhs) const { return !(rhs == *this); }

  uint64_t Hash() const {
    static constexpr uint32_t seed = 12345;
    uint64_t h1 = MurmurHash3_x86_32(&k1, sizeof(uint32_t), seed);
    uint64_t h2 = MurmurHash3_x86_32(&k2, sizeof(uint32_t), seed);
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }

  friend std::ostream &operator<<(std::ostream &os, const Key &k) {
    os << "Key[" << k.k1 << "," << k.k2 << "]";
    return os;
  }
};

/**
 * The test value used in the hash table
 */
struct Value {
  uint32_t v1, v2, v3, v4;
  bool operator==(const Value &rhs) const {
    return v1 == rhs.v1 && v2 == rhs.v2 && v3 == rhs.v3 && v4 == rhs.v4;
  }
  bool operator!=(const Value &rhs) const { return !(rhs == *this); }
};

/**
 * The base hash table test class
 */
class HashTableTest : public PelotonTest {
 public:
  HashTableTest() : pool_(new ::peloton::type::EphemeralPool()) {}

  type::AbstractPool &GetMemPool() const { return *pool_; }

 private:
  std::unique_ptr<::peloton::type::AbstractPool> pool_;
};

TEST_F(HashTableTest, CanInsertUniqueKeys) {
  codegen::util::HashTable table{GetMemPool(), sizeof(Key), sizeof(Value)};

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;

  std::vector<Key> keys;

  // Insert keys
  for (uint32_t i = 0; i < to_insert; i++) {
    Key k{1, i};
    Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
    table.TypedInsert(k.Hash(), k, v);

    keys.emplace_back(k);
  }

  EXPECT_EQ(to_insert, table.NumElements());

  // Lookup
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(1, count) << "Found duplicate keys in unique key test";
  }
}

TEST_F(HashTableTest, CanInsertDuplicateKeys) {
  codegen::util::HashTable table{GetMemPool(), sizeof(Key), sizeof(Value)};

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;
  constexpr uint32_t max_dups = 4;

  std::vector<Key> keys;

  // Insert keys
  uint32_t num_inserts = 0;
  for (uint32_t i = 0; i < to_insert; i++) {
    // Choose a random number of duplicates to insert. Store this in the k1.
    uint32_t num_dups = 1 + (rand() % max_dups);
    Key k{num_dups, i};

    // Duplicate insertion
    for (uint32_t dup = 0; dup < num_dups; dup++) {
      Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
      table.TypedInsert(k.Hash(), k, v);
      num_inserts++;
    }

    keys.emplace_back(k);
  }

  EXPECT_EQ(num_inserts, table.NumElements());

  // Lookup
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(key.k1, count) << key << " found " << count << " dups ...";
  }
}

TEST_F(HashTableTest, CanInsertLazilyWithDups) {
  codegen::util::HashTable table{GetMemPool(), sizeof(Key), sizeof(Value)};

  constexpr uint32_t to_insert = 50000;
  constexpr uint32_t c1 = 4444;
  constexpr uint32_t max_dups = 4;

  std::vector<Key> keys;

  // Insert keys
  uint32_t num_inserts = 0;
  for (uint32_t i = 0; i < to_insert; i++) {
    // Choose a random number of duplicates to insert. Store this in the k1.
    uint32_t num_dups = 1 + (rand() % max_dups);
    Key k{num_dups, i};

    // Duplicate insertion
    for (uint32_t dup = 0; dup < num_dups; dup++) {
      Value v = {.v1 = k.k2, .v2 = 2, .v3 = 3, .v4 = c1};
      table.TypedInsertLazy(k.Hash(), k, v);
      num_inserts++;
    }

    keys.emplace_back(k);
  }

  // Number of elements should reflect lazy insertions
  EXPECT_EQ(num_inserts, table.NumElements());
  EXPECT_LT(table.Capacity(), table.NumElements());

  // Build lazy
  table.BuildLazy();

  // Lookups should succeed
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(c1, v.v4) << "Value's [v4] doesn't match constant";
      count++;
    };
    table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(key.k1, count) << key << " found " << count << " dups ...";
  }
}

TEST_F(HashTableTest, ParallelMerge) {
  codegen::util::HashTable global_table{GetMemPool(), sizeof(Key),
                                        sizeof(Value)};

  constexpr uint32_t num_threads = 4;
  constexpr uint32_t to_insert = 20000;

  std::mutex keys_mutex;
  std::vector<Key> keys;

  executor::ExecutorContext exec_ctx{nullptr};

  // Allocate hash tables for each thread
  auto &thread_states = exec_ctx.GetThreadStates();
  thread_states.Reset(sizeof(codegen::util::HashTable));
  thread_states.Allocate(num_threads);

  auto add_key = [&keys_mutex, &keys](const Key &k) {
    std::lock_guard<std::mutex> lock{keys_mutex};
    keys.emplace_back(k);
  };

  // Insert function
  auto insert_fn = [&add_key, &exec_ctx, &to_insert](uint64_t tid) {
    // Get the local table for this thread
    auto *table = reinterpret_cast<codegen::util::HashTable *>(
        exec_ctx.GetThreadStates().AccessThreadState(tid));

    // Initialize it
    codegen::util::HashTable::Init(*table, exec_ctx, sizeof(Key),
                                   sizeof(Value));

    // Insert keys disjoint from other threads
    for (uint32_t i = tid * to_insert, end = i + to_insert; i != end; i++) {
      Key k{static_cast<uint32_t>(tid), i};
      Value v = {.v1 = k.k2, .v2 = k.k1, .v3 = 3, .v4 = 4444};
      table->TypedInsertLazy(k.Hash(), k, v);

      add_key(k);
    }
  };

  auto merge_fn = [&global_table, &thread_states](uint64_t tid) {
    // Get the local table for this threads
    auto *table = reinterpret_cast<codegen::util::HashTable *>(
        thread_states.AccessThreadState(tid));

    // Merge it into the global table
    global_table.MergeLazyUnfinished(*table);
  };

  // First insert into thread local tables in parallel
  LaunchParallelTest(num_threads, insert_fn);
  for (uint32_t tid = 0; tid < num_threads; tid++) {
    auto *ht = reinterpret_cast<codegen::util::HashTable *>(
        thread_states.AccessThreadState(tid));
    EXPECT_EQ(to_insert, ht->NumElements());
  }

  // Now resize global table
  global_table.ReserveLazy(thread_states, 0);
  EXPECT_EQ(NextPowerOf2(keys.size()), global_table.Capacity());

  // Now merge thread-local tables into global table in parallel
  LaunchParallelTest(num_threads, merge_fn);

  // Now probe global
  EXPECT_EQ(to_insert * num_threads, global_table.NumElements());
  EXPECT_LE(global_table.NumElements(), global_table.Capacity());
  for (const auto &key : keys) {
    uint32_t count = 0;
    std::function<void(const Value &v)> f = [&key, &count](const Value &v) {
      EXPECT_EQ(key.k2, v.v1)
          << "Value's [v1] found in table doesn't match insert key";
      EXPECT_EQ(key.k1, v.v2) << "Key " << key << " inserted by thread "
                              << key.k1 << " but value was inserted by thread "
                              << v.v2;
      count++;
    };
    global_table.TypedProbe(key.Hash(), key, f);
    EXPECT_EQ(1, count) << "Found duplicate keys in unique key test";
  }
}

}  // namespace test
}  // namespace peloton