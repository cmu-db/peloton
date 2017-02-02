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

#include "index/index_factory.h"
#include "storage/tuple.h"
#include "type/types.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// IndexTestsUtil
//===--------------------------------------------------------------------===//

class IndexTestsUtil {
 public:
  /*
   * Builds an index with 4 columns, the first 2 being indexed
   */
  static index::Index *BuildIndex(const IndexType index_type,
                                  const bool unique_keys);

  /**
   * Insert helper function
   */
  static void InsertTest(index::Index *index, type::AbstractPool *pool,
                         size_t scale_factor,
                         UNUSED_ATTRIBUTE uint64_t thread_itr);
};
}  // namespace test
}  // namespace peloton
