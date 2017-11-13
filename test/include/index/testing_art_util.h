//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_util.h
//
// Identification: test/include/index/testing_art_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "concurrency/transaction.h"
#include "storage/data_table.h"
#include "index/art_index.h"
#include "index/art.h"
#include "index/index.h"
#include <map>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TestingARTUtil
//===--------------------------------------------------------------------===//

class TestingArtUtil {
public:
  //===--------------------------------------------------------------------===//
  // Test Cases
  //===--------------------------------------------------------------------===//

  static void BasicTest(const IndexType index_type);

//  static void MultiMapInsertTest(const IndexType index_type);
//
//  static void UniqueKeyInsertTest(const IndexType index_type);
//
//  static void UniqueKeyDeleteTest(const IndexType index_type);
//
  static void NonUniqueKeyDeleteTest(const IndexType index_type);

  static void MultiThreadedInsertTest(const IndexType index_type);

//  static void UniqueKeyMultiThreadedTest(const IndexType index_type);
//
//  static void NonUniqueKeyMultiThreadedTest(const IndexType index_type);

  static void NonUniqueKeyMultiThreadedScanTest(const IndexType index_type);

  static void NonUniqueKeyMultiThreadedStressTest(const IndexType index_type);
//
//  static void NonUniqueKeyMultiThreadedStressTest2(const IndexType index_type);

  //===--------------------------------------------------------------------===//
  // Utility Methods
  //===--------------------------------------------------------------------===//
  static storage::DataTable *CreateTable(int tuples_per_tilegroup_count = 5,
    bool indexes = true, oid_t table_oid = INVALID_OID);

  /**
   * Insert helper function
   */
  static void InsertHelper(storage::DataTable *table,
                           type::AbstractPool *testing_pool, size_t scale_factor, int num_rows, bool random,
                           std::vector<storage::Tuple *> *keys, std::vector<ItemPointer *> *expected_values,
                           UNUSED_ATTRIBUTE uint64_t thread_itr);

  static void InsertHelperMicroBench(index::ArtIndex *index,
                                     int num_rows, UNUSED_ATTRIBUTE uint64_t thread_itr);

  /**
   * Delete helper function
   */
  static void DeleteHelper(storage::DataTable *table, int num_rows,
                           std::vector<storage::Tuple *> keys, std::vector<ItemPointer *> expected_values,
                           UNUSED_ATTRIBUTE uint64_t thread_itr);

  static void DeleteHelperMicroBench(index::ArtIndex *index,
                                     int num_rows, UNUSED_ATTRIBUTE uint64_t thread_itr);

  static void ScanHelperMicroBench(index::ArtIndex *index, size_t scale_factor,
                                     int total_rows, int insert_workers, UNUSED_ATTRIBUTE uint64_t thread_itr);

  struct KeyAndValues {
    std::array<uint64_t, 16> values;
    index::ARTKey key;
    storage::Tuple *tuple;
  };
  static std::array<KeyAndValues, 10000> key_to_values;
  static std::map<index::TID, index::ARTKey *> value_to_key;
  static bool map_populated;

  static void PopulateMap(index::Index &index);
};


}
}
