//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_util.h
//
// Identification: test/include/index/testing_index_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "common/internal_types.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TestingIndexUtil
//===--------------------------------------------------------------------===//

class TestingIndexUtil {
 public:
  //===--------------------------------------------------------------------===//
  // Test Cases
  //===--------------------------------------------------------------------===//

  static void BasicTest(IndexType index_type);

  static void MultiMapInsertTest(IndexType index_type);

  static void UniqueKeyInsertTest(IndexType index_type);

  static void UniqueKeyDeleteTest(IndexType index_type);

  static void NonUniqueKeyDeleteTest(IndexType index_type);

  static void MultiThreadedInsertTest(IndexType index_type);

  static void UniqueKeyMultiThreadedTest(IndexType index_type);

  static void NonUniqueKeyMultiThreadedTest(IndexType index_type);

  static void NonUniqueKeyMultiThreadedStressTest(IndexType index_type);

  static void NonUniqueKeyMultiThreadedStressTest2(IndexType index_type);

  //===--------------------------------------------------------------------===//
  // Utility Methods
  //===--------------------------------------------------------------------===//

  // Builds an index with 4 columns, the first 2 being indexed
  static std::unique_ptr<index::IndexMetadata> BuildTestIndexMetadata(
      const IndexType index_type, const bool unique_keys);

  static index::Index *BuildIndex(IndexType index_type, bool unique_keys);

  static void DestroyIndex(index::Index *index);

  // Insert helper function
  static void InsertHelper(index::Index *index, type::AbstractPool *pool,
                           size_t scale_factor, uint64_t thread_itr);

  // Delete helper function
  static void DeleteHelper(index::Index *index, type::AbstractPool *pool,
                           size_t scale_factor, uint64_t thread_itr);

  static std::shared_ptr<ItemPointer> item0;
  static std::shared_ptr<ItemPointer> item1;
  static std::shared_ptr<ItemPointer> item2;
};
}  // namespace test
}  // namespace peloton
