//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_test.cpp
//
// Identification: test/index/index_util_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "gtest/gtest.h"
#include "common/harness.h"

#include "common/logger.h"
#include "common/platform.h"
#include "index/index_factory.h"
#include "storage/tuple.h"
#include "index/index_util.h"

namespace peloton {
namespace test {

class IndexUtilTests : public PelotonTest {};

// These two are here since the IndexMetadata object does not claim
// ownership for the two schema objects so they will be not destroyed
// automatically
//
// Put them here to avoid Valgrind warning
static catalog::Schema *tuple_schema = nullptr;
static catalog::Schema *key_schema = nullptr;

/*
 * BuildIndex() - Builds an index with 4 columns
 *
 * The index has 4 columns as tuple key (A, B, C, D), and three of them
 * are indexed:
 *
 * tuple key: 0 1 2 3
 * index key: 3 0   1 (i.e. the 1st column of index key is the 3rd column of
 *                     tuple key)
 */
static const index::Index *BuildIndex() {
  // Build tuple and key schema
  std::vector<catalog::Column> tuple_column_list{};
  std::vector<catalog::Column> index_column_list{};

  // The following key are both in index key and tuple key and they are
  // indexed
  // The size of the key is:
  //   integer 4 * 3 = total 12

  catalog::Column column0(VALUE_TYPE_INTEGER,
                          GetTypeSize(VALUE_TYPE_INTEGER),
                          "A",
                          true);

  catalog::Column column1(VALUE_TYPE_VARCHAR,
                          1024,
                          "B",
                          false);

  // The following twoc constitutes tuple schema but does not appear in index

  catalog::Column column2(VALUE_TYPE_DOUBLE,
                          GetTypeSize(VALUE_TYPE_DOUBLE),
                          "C",
                          true);

  catalog::Column column3(VALUE_TYPE_INTEGER,
                          GetTypeSize(VALUE_TYPE_INTEGER),
                          "D",
                          true);

  // Use all four columns to build tuple schema

  tuple_column_list.push_back(column0);
  tuple_column_list.push_back(column1);
  tuple_column_list.push_back(column2);
  tuple_column_list.push_back(column3);

  tuple_schema = new catalog::Schema(tuple_column_list);
  
  // Use column 3, 0, 1 to build index key
  
  index_column_list.push_back(column3);
  index_column_list.push_back(column0);
  index_column_list.push_back(column1);

  // This will be copied into the key schema as well as into the IndexMetadata
  // object to identify indexed columns
  std::vector<oid_t> key_attrs = {3, 0, 1};

  // key schame also needs the mapping relation from index key to tuple key
  key_schema = new catalog::Schema(index_column_list);
  key_schema->SetIndexedColumns(key_attrs);

  // Build index metadata
  //
  // NOTE: Since here we use a relatively small key (size = 12)
  // so index_test is only testing with a certain kind of key
  // (most likely, GenericKey)
  //
  // For testing IntsKey and TupleKey we need more test cases
  index::IndexMetadata *index_metadata = \
    new index::IndexMetadata("index_util_test",
                             88888,                       // Index oid
                             INDEX_TYPE_BWTREE,
                             INDEX_CONSTRAINT_TYPE_DEFAULT,
                             tuple_schema,
                             key_schema,
                             key_attrs,
                             true);                      // unique_keys

  // Build index
  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);

  return index;
}

/*
 * IsPointQueryTest() - Tests whether the index util correctly recognizes
 *                      point query
 *
 * The index configuration is as follows:
 *
 * tuple key: 0 1 2 3
 * index_key: 3 0 1
 */
TEST_F(IndexUtilTests, IsPointQueryTest) {
  const index::Index *index_p = BuildIndex();
  bool ret;
  
  // Test basic
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {3, 0, 1},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, true);
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {1, 0, 3},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, true);
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {0, 1, 3},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, true);
  
  // Test whether reconizes if only two columns are matched
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {0, 1},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, false);
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {3, 0},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, false);
  
  // Test empty
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {},
                     {});
  EXPECT_EQ(ret, false);
  
  // Test redundant conditions
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {0, 3, 3, 0, 3, 1},
                     {EXPRESSION_TYPE_COMPARE_LESSTHAN,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_LESSTHAN,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, true);
  
  // Test duplicated conditions on a single column
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {3, 3, 3},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_EQUAL});
  EXPECT_EQ(ret, false);
  
  //
  // The last test should logically be classified as point query
  // but our procedure does not give positive result to reduce
  // the complexity
  //
  
  ret = IsPointQuery(index_p->GetMetadata(),
                     {3, 0, 1, 0},
                     {EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
                      EXPRESSION_TYPE_COMPARE_EQUAL,
                      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO});
  EXPECT_EQ(ret, false);
  
  return;
}
  
}  // End test namespace
}  // End peloton namespace
