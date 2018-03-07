//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index_test.cpp
//
// Identification: test/index/art_index_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gmock/gtest/gtest.h"

#include "index/art_index.h"
#include "index/testing_index_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

using KeyPtr = std::unique_ptr<storage::Tuple>;
using ItemPtr = std::unique_ptr<ItemPointer>;

class TestEntry {
 private:
  KeyPtr key;
  ItemPtr val;

 public:
  TestEntry(KeyPtr &&_key, ItemPtr &&_val)
      : key(std::move(_key)), val(std::move(_val)) {}

  storage::Tuple *GetKey() const { return key.get(); }
  ItemPointer *GetVal() const { return val.get(); }
};

using Table = std::vector<TestEntry>;
using IndexPtr = std::unique_ptr<index::Index, void (*)(index::Index *)>;

// By default, an ART index performs key lookups from a base data table to
// handle path compression and lazy expansion. To isolate the index and storage
// components during testing, we don't want to bring up a full table. Instead,
// we specialize the index to perform key lookups from an in-memory vector of
// keys.
//
// When inserting key-value pairs into the index, the ItemPointer we use is just
// an index into the backing vector to find the key.

class ArtIndexForTest : public index::ArtIndex {
  const Table &data_;

  static void LoadKey(void *ctx, TID tid, art::Key &key) {
    auto *index = reinterpret_cast<ArtIndexForTest *>(ctx);
    auto *ip = reinterpret_cast<const ItemPointer *>(tid);
    ASSERT_TRUE(ip->offset < index->data_.size());
    index->ConstructArtKey(*index->data_[ip->offset].GetKey(), key);
  }

 public:
  ArtIndexForTest(index::IndexMetadata *metadata, const Table &data)
      : ArtIndex(metadata), data_(data) {
    // The key loading function loads from an in-memory vector
    SetLoadKeyFunc(LoadKey, reinterpret_cast<void *>(this));
  }
};

// The base test class
class ArtIndexTests : public PelotonTest {
 public:
  ArtIndexTests() : index_(CreateTestIndex()) { GenerateTestInput(1); }

  IndexPtr CreateTestIndex() const {
    auto meta = TestingIndexUtil::BuildTestIndexMetadata(IndexType::ART, false);
    return IndexPtr{new ArtIndexForTest(meta.release(), data_),
                    TestingIndexUtil::DestroyIndex};
  }

  // Get the test index
  index::Index &GetTestIndex() const { return *index_; }

  // Get the test data
  const Table &GetTestData() const { return data_; }

  KeyPtr CreateIndexKey(uint32_t col_a, const std::string &col_b) {
    auto pool = TestingHarness::GetInstance().GetTestingPool();
    KeyPtr key{new storage::Tuple(index_->GetKeySchema(), true)};
    key->SetValue(0, type::ValueFactory::GetIntegerValue(col_a), pool);
    key->SetValue(1, type::ValueFactory::GetVarcharValue(col_b), pool);
    return key;
  }

  ItemPtr CreateItemPointer(size_t position) {
    return ItemPtr{new ItemPointer(0, static_cast<uint32_t>(position))};
  }

  void GenerateTestInput(uint32_t scale_factor);

  static void InsertHelper(index::Index *index,
                           const std::vector<TestEntry> *data,
                           UNUSED_ATTRIBUTE uint64_t thread_num) {
    // Insert original data
    for (const auto &entry : *data) {
      index->InsertEntry(entry.GetKey(), entry.GetVal());
    }
    // Now try again, all these are duplicates and should fail
    for (const auto &entry : *data) {
      index->InsertEntry(entry.GetKey(), entry.GetVal());
    }
  }

  static void DeleteHelper(index::Index *index,
                           const std::vector<TestEntry> *data,
                           ItemPointer *dummy_tid,
                           UNUSED_ATTRIBUTE uint64_t thread_num) {
    for (uint32_t i = 0; i < data->size(); i += 7) {
      auto &key0_test = data->at(i);
      auto &key1_test = data->at(i + 1);
      auto &key2_test = data->at(i + 4);
      auto &key3_test = data->at(i + 5);
      auto &key4_test = data->at(i + 6);

      index->DeleteEntry(key0_test.GetKey(), key0_test.GetVal());
      index->DeleteEntry(key1_test.GetKey(), key1_test.GetVal());
      index->DeleteEntry(key2_test.GetKey(), dummy_tid);
      index->DeleteEntry(key3_test.GetKey(), key3_test.GetVal());
      index->DeleteEntry(key4_test.GetKey(), dummy_tid);
    }
  }

 private:
  IndexPtr index_;
  std::vector<TestEntry> data_;
};

void ArtIndexTests::GenerateTestInput(uint32_t scale_factor) {
  data_.clear();
  for (uint32_t scale = 1; scale <= scale_factor; scale++) {
    // Key (100, a)
    data_.emplace_back(CreateIndexKey(100 * scale, "a"),
                       CreateItemPointer(data_.size()));

    // Key (100, b)
    data_.emplace_back(CreateIndexKey(100 * scale, "b"),
                       CreateItemPointer(data_.size()));
    data_.emplace_back(CreateIndexKey(100 * scale, "b"),
                       CreateItemPointer(data_.size()));
    data_.emplace_back(CreateIndexKey(100 * scale, "b"),
                       CreateItemPointer(data_.size()));

    // Key (100, c)
    data_.emplace_back(CreateIndexKey(100 * scale, "c"),
                       CreateItemPointer(data_.size()));

    // Key (400, d)
    data_.emplace_back(CreateIndexKey(400 * scale, "d"),
                       CreateItemPointer(data_.size()));

    // Key (500, eee...)
    data_.emplace_back(
        CreateIndexKey(500 * scale, StringUtil::Repeat("e", 1000)),
        CreateItemPointer(data_.size()));
  }
}

TEST_F(ArtIndexTests, BasicTest) {
  std::vector<ItemPointer *> location_ptrs;

  // The index
  auto &index = GetTestIndex();

  // The test key and value
  auto &test_data = GetTestData()[0];
  auto *test_key = test_data.GetKey();
  auto *test_val = test_data.GetVal();

  // Insert and check
  index.InsertEntry(test_key, test_val);
  index.ScanKey(test_key, location_ptrs);
  ASSERT_EQ(1, location_ptrs.size());
  EXPECT_EQ(test_val->block, location_ptrs[0]->block);
  location_ptrs.clear();

  // Delete and check
  index.DeleteEntry(test_key, test_val);
  index.ScanKey(test_key, location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();
}

TEST_F(ArtIndexTests, NonUniqueKeyInsertTest) {
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  auto &index = GetTestIndex();
  auto &test_data = GetTestData();

  // Single threaded test
  LaunchParallelTest(1, ArtIndexTests::InsertHelper, &index, &test_data);

  // Checks
  index.ScanAllKeys(location_ptrs);
  EXPECT_EQ(7, location_ptrs.size());
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0 = CreateIndexKey(100, "a");
  std::unique_ptr<storage::Tuple> keynonce = CreateIndexKey(1000, "f");

  index.ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index.ScanKey(key0.get(), location_ptrs);
  ASSERT_EQ(1, location_ptrs.size());
  EXPECT_EQ(test_data[0].GetVal()->block, location_ptrs[0]->block);
  location_ptrs.clear();
}

TEST_F(ArtIndexTests, NonUniqueKeyDeleteTest) {
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  auto &index = GetTestIndex();
  auto &test_data = GetTestData();

  std::unique_ptr<ItemPointer> dummy_tid{new ItemPointer()};

  // Single threaded test
  LaunchParallelTest(1, ArtIndexTests::InsertHelper, &index, &test_data);
  LaunchParallelTest(1, ArtIndexTests::DeleteHelper, &index, &test_data,
                     dummy_tid.get());

  // Checks
  index.ScanAllKeys(location_ptrs);
  EXPECT_EQ(4, location_ptrs.size());
  location_ptrs.clear();

  // Check keys
  std::unique_ptr<storage::Tuple> key0 = CreateIndexKey(100, "a");
  index.ScanKey(key0.get(), location_ptrs);
  ASSERT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key1 = CreateIndexKey(100, "b");
  index.ScanKey(key1.get(), location_ptrs);
  ASSERT_EQ(2, location_ptrs.size());
  location_ptrs.clear();

  // Delete all (100,b) keys
  for (const auto &test_entry : test_data) {
    if (test_entry.GetKey()->EqualsNoSchemaCheck(*key1)) {
      index.DeleteEntry(test_entry.GetKey(), test_entry.GetVal());
    }
  }
  index.ScanKey(key1.get(), location_ptrs);
  ASSERT_EQ(0, location_ptrs.size());
}

TEST_F(ArtIndexTests, NonUniqueKeyMultiThreadedInsertTest) {
  std::vector<ItemPointer *> location_ptrs;

  // INDEX
  auto &index = GetTestIndex();
  auto &test_data = GetTestData();

  // Single threaded test
  size_t num_threads = 4;
  LaunchParallelTest(num_threads, ArtIndexTests::InsertHelper, &index,
                     &test_data);

  // Checks
  index.ScanAllKeys(location_ptrs);
  EXPECT_EQ(7, location_ptrs.size());
  location_ptrs.clear();

  std::unique_ptr<storage::Tuple> key0 = CreateIndexKey(100, "a");
  std::unique_ptr<storage::Tuple> keynonce = CreateIndexKey(1000, "f");

  index.ScanKey(keynonce.get(), location_ptrs);
  EXPECT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  index.ScanKey(key0.get(), location_ptrs);
  ASSERT_EQ(1, location_ptrs.size());
  EXPECT_EQ(test_data[0].GetVal()->block, location_ptrs[0]->block);
  location_ptrs.clear();
}

TEST_F(ArtIndexTests, NonUniqueKeyMultiThreadedStressTest) {
  std::vector<ItemPointer *> location_ptrs;

  uint32_t scale_factor = 20;
  uint32_t num_threads = 4;
  GenerateTestInput(scale_factor);

  // INDEX
  auto &index = GetTestIndex();
  auto &test_data = GetTestData();

  std::unique_ptr<ItemPointer> dummy_tid{new ItemPointer()};

  // Single threaded test
  LaunchParallelTest(num_threads, ArtIndexTests::InsertHelper, &index,
                     &test_data);
  LaunchParallelTest(num_threads, ArtIndexTests::DeleteHelper, &index,
                     &test_data, dummy_tid.get());

  // Checks
  index.ScanAllKeys(location_ptrs);
  EXPECT_EQ(4 * scale_factor, location_ptrs.size());
  location_ptrs.clear();

  // Check keys
  std::unique_ptr<storage::Tuple> key0 = CreateIndexKey(100, "a");
  index.ScanKey(key0.get(), location_ptrs);
  ASSERT_EQ(0, location_ptrs.size());
  location_ptrs.clear();

  for (uint32_t i = 1; i <= scale_factor; i++) {
    std::unique_ptr<storage::Tuple> key1 = CreateIndexKey(100 * i, "b");
    index.ScanKey(key1.get(), location_ptrs);
    ASSERT_EQ(2, location_ptrs.size());
    location_ptrs.clear();
  }
}

}  // namespace test
}  // namespace peloton