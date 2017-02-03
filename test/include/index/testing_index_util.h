//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_tests_util.h
//
// Identification: test/include/sql/sql_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/macros.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "type/types.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TestingIndexUtil
//===--------------------------------------------------------------------===//

class TestingIndexUtil {
 public:
  /*
   * Builds an index with 4 columns, the first 2 being indexed
   */
  static index::Index *BuildIndex(const IndexType index_type,
                                  const bool unique_keys);

  /**
   * Insert helper function
   */
  static void InsertHelper(index::Index *index, type::AbstractPool *pool,
                         size_t scale_factor,
                         UNUSED_ATTRIBUTE uint64_t thread_itr);

  /**
   * Delete helper function
   */
  static void DeleteHelper(index::Index *index, type::AbstractPool *pool,
                         size_t scale_factor,
                         UNUSED_ATTRIBUTE uint64_t thread_itr);

  static std::shared_ptr<ItemPointer> item0;
  static std::shared_ptr<ItemPointer> item1;
  static std::shared_ptr<ItemPointer> item2;
};
}  // namespace test
}  // namespace peloton
