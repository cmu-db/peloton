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

#include "common/harness.h"
#include "gtest/gtest.h"

#include "common/logger.h"
#include "common/platform.h"
#include "index/index_factory.h"
#include "index/index_util.h"
#include "index/scan_optimizer.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

using namespace index;
using namespace storage;

class IndexUtilTests : public PelotonTest {};

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

// These two are here since the IndexMetadata object does not claim
// ownership for the two schema objects so they will be not destroyed
// automatically
//
// Put them here to avoid Valgrind warning
static std::unique_ptr<catalog::Schema> tuple_schema = nullptr;

static index::Index *BuildIndex() {
  // Build tuple and key schema
  std::vector<catalog::Column> tuple_column_list{};
  std::vector<catalog::Column> index_column_list{};

  // The following key are both in index key and tuple key and they are
  // indexed
  // The size of the key is:
  //   integer 4 * 3 = total 12

  catalog::Column column0(common::Type::INTEGER,
                          common::Type::GetTypeSize(common::Type::INTEGER), "A",
                          true);

  catalog::Column column1(common::Type::VARCHAR, 1024, "B", false);

  // The following twoc constitutes tuple schema but does not appear in index

  catalog::Column column2(common::Type::DECIMAL,
                          common::Type::GetTypeSize(common::Type::DECIMAL), "C",
                          true);

  catalog::Column column3(common::Type::INTEGER,
                          common::Type::GetTypeSize(common::Type::INTEGER), "D",
                          true);

  // Use all four columns to build tuple schema

  tuple_column_list.push_back(column0);
  tuple_column_list.push_back(column1);
  tuple_column_list.push_back(column2);
  tuple_column_list.push_back(column3);

  if (tuple_schema == nullptr) {
    tuple_schema.reset(new catalog::Schema(tuple_column_list));
  }

  // Use column 3, 0, 1 to build index key

  index_column_list.push_back(column3);
  index_column_list.push_back(column0);
  index_column_list.push_back(column1);

  // This will be copied into the key schema as well as into the IndexMetadata
  // object to identify indexed columns
  std::vector<oid_t> key_attrs = {3, 0, 1};

  // key schame also needs the mapping relation from index key to tuple key
  auto key_schema = new catalog::Schema(index_column_list);
  key_schema->SetIndexedColumns(key_attrs);

  // Build index metadata
  //
  // NOTE: Since here we use a relatively small key (size = 12)
  // so index_test is only testing with a certain kind of key
  // (most likely, GenericKey)
  //
  // For testing IntsKey and TupleKey we need more test cases
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "index_util_test", 88888,  // Index oid
      INVALID_OID, INVALID_OID, INDEX_TYPE_BWTREE,
      INDEX_CONSTRAINT_TYPE_DEFAULT, tuple_schema.get(), key_schema, key_attrs,
      true);  // unique_keys

  // Build index
  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  // Actually this will never be hit since if index creation fails an exception
  // would be raised (maybe out of memory would result in a nullptr? Anyway
  // leave it here)
  EXPECT_TRUE(index != NULL);

  return index;
}

/*
 * FindValueIndexTest() - Tests whether the index util correctly recognizes
 *                        point query
 *
 * The index configuration is as follows:
 *
 * tuple key: 0 1 2 3
 * index_key: 3 0 1
 */
TEST_F(IndexUtilTests, FindValueIndexTest) {
  const index::Index *index_p = BuildIndex();
  bool ret;

  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  // Test basic

  ret = FindValueIndex(
      index_p->GetMetadata(), {3, 0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, true);
  value_index_list.clear();

  ret = FindValueIndex(
      index_p->GetMetadata(), {1, 0, 3},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, true);
  value_index_list.clear();

  ret = FindValueIndex(
      index_p->GetMetadata(), {0, 1, 3},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, true);
  value_index_list.clear();

  // Test whether reconizes if only two columns are matched

  ret = FindValueIndex(
      index_p->GetMetadata(), {0, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  ret = FindValueIndex(
      index_p->GetMetadata(), {3, 0},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  // Test empty

  ret = FindValueIndex(index_p->GetMetadata(), {}, {}, value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  // Test redundant conditions

  // This should return false, since the < already defines a lower bound
  ret = FindValueIndex(
      index_p->GetMetadata(), {0, 3, 3, 0, 3, 1},
      {EXPRESSION_TYPE_COMPARE_LESSTHAN, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_LESSTHAN, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  // This should return true
  ret = FindValueIndex(
      index_p->GetMetadata(), {0, 3, 3, 0, 3, 1},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_LESSTHAN, EXPRESSION_TYPE_COMPARE_LESSTHAN,
       EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, true);
  value_index_list.clear();

  // Test duplicated conditions on a single column

  ret = FindValueIndex(
      index_p->GetMetadata(), {3, 3, 3},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_EQUAL},
      value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  //
  // The last test should logically be classified as point query
  // but our procedure does not give positive result to reduce
  // the complexity
  //

  ret = FindValueIndex(
      index_p->GetMetadata(), {3, 0, 1, 0},
      {EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
       EXPRESSION_TYPE_COMPARE_EQUAL,
       EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO},
      value_index_list);
  EXPECT_EQ(ret, false);
  value_index_list.clear();

  delete index_p;

  return;
}

/*
 * ConstructBoundaryKeyTest() - Tests ConstructBoundaryKey() function for
 *                              conjunctions
 */
TEST_F(IndexUtilTests, ConstructBoundaryKeyTest) {
  index::Index *index_p = BuildIndex();

  // This is the output variable
  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  std::vector<common::Value> value_list{};
  std::vector<oid_t> tuple_column_id_list{};
  std::vector<ExpressionType> expr_list{};

  value_list = {
      common::ValueFactory::GetIntegerValue(100).Copy(),
      common::ValueFactory::GetIntegerValue(200).Copy(),
      common::ValueFactory::GetIntegerValue(50).Copy(),
  };

  tuple_column_id_list = {3, 3, 0};

  expr_list = {
      EXPRESSION_TYPE_COMPARE_GREATERTHAN,
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
  };

  IndexScanPredicate isp{};

  isp.AddConjunctionScanPredicate(index_p, value_list, tuple_column_id_list,
                                  expr_list);

  const std::vector<ConjunctionScanPredicate> &cl = isp.GetConjunctionList();

  // First check the conjunction has been pushed into the scan predicate object
  EXPECT_EQ(cl.size(), 1UL);

  // Then make sure all values have been bound (i.e. no free variable)
  EXPECT_EQ(cl[0].GetBindingCount(), 0UL);

  // Check whether the entire predicate is full index scan (should not be)
  EXPECT_EQ(isp.IsFullIndexScan(), false);

  // Then check the conjunction predicate
  EXPECT_EQ(cl[0].IsFullIndexScan(), false);

  // Then check whether the conjunction predicate is a point query
  EXPECT_EQ(cl[0].IsPointQuery(), false);

  LOG_INFO("Low key = %s", cl[0].GetLowKey()->GetInfo().c_str());
  LOG_INFO("High key = %s", cl[0].GetHighKey()->GetInfo().c_str());

  ///////////////////////////////////////////////////////////////////
  // Test the case where there is no optimization that could be done
  //
  // The condition means first index column does not equal 100
  ///////////////////////////////////////////////////////////////////

  value_list = {
      common::ValueFactory::GetIntegerValue(100).Copy(),
  };

  tuple_column_id_list = {
      3,
  };

  expr_list = {
      EXPRESSION_TYPE_COMPARE_NOTEQUAL,
  };

  isp.AddConjunctionScanPredicate(index_p, value_list, tuple_column_id_list,
                                  expr_list);

  const std::vector<ConjunctionScanPredicate> &cl2 = isp.GetConjunctionList();

  EXPECT_EQ(cl2.size(), 2UL);
  EXPECT_EQ(cl2[1].GetBindingCount(), 0UL);
  EXPECT_EQ(isp.IsFullIndexScan(), true);
  EXPECT_EQ(cl2[1].IsFullIndexScan(), true);
  EXPECT_EQ(cl2[1].IsPointQuery(), false);

  ///////////////////////////////////////////////////////////////////
  // Test point query and query key
  //
  // Index key = <100, 50, "Peloton!">
  ///////////////////////////////////////////////////////////////////

  IndexScanPredicate isp2{};

  value_list = {
      common::ValueFactory::GetIntegerValue(100).Copy(),
      common::ValueFactory::GetVarcharValue("Peloton!").Copy(),
      common::ValueFactory::GetIntegerValue(50).Copy(),
  };

  tuple_column_id_list = {3, 1, 0};

  expr_list = {
      EXPRESSION_TYPE_COMPARE_EQUAL, EXPRESSION_TYPE_COMPARE_EQUAL,
      EXPRESSION_TYPE_COMPARE_EQUAL,
  };

  isp2.AddConjunctionScanPredicate(index_p, value_list, tuple_column_id_list,
                                   expr_list);

  const std::vector<ConjunctionScanPredicate> &cl3 = isp2.GetConjunctionList();

  EXPECT_EQ(cl3.size(), 1UL);
  EXPECT_EQ(cl3[0].GetBindingCount(), 0UL);
  EXPECT_EQ(isp2.IsFullIndexScan(), false);
  EXPECT_EQ(cl3[0].IsFullIndexScan(), false);
  EXPECT_EQ(cl3[0].IsPointQuery(), true);

  LOG_INFO("Point query key = %s",
           cl3[0].GetPointQueryKey()->GetInfo().c_str());

  ///////////////////////////////////////////////////////////////////
  // End of all test cases
  ///////////////////////////////////////////////////////////////////
  delete index_p;

  return;
}

/*
 * BindKeyTest() - Tests binding values onto keys that are not bound
 */
TEST_F(IndexUtilTests, BindKeyTest) {
  index::Index *index_p = BuildIndex();

  // This is the output variable
  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  std::vector<common::Value> value_list{};
  std::vector<oid_t> tuple_column_id_list{};
  std::vector<ExpressionType> expr_list{};

  value_list = {
      common::ValueFactory::GetParameterOffsetValue(2).Copy(),
      common::ValueFactory::GetParameterOffsetValue(0).Copy(),
      common::ValueFactory::GetParameterOffsetValue(1).Copy(),
  };

  tuple_column_id_list = {3, 3, 0};

  expr_list = {
      EXPRESSION_TYPE_COMPARE_GREATERTHAN,
      EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
  };

  IndexScanPredicate isp{};

  isp.AddConjunctionScanPredicate(index_p, value_list, tuple_column_id_list,
                                  expr_list);

  const std::vector<ConjunctionScanPredicate> &cl = isp.GetConjunctionList();

  ///////////////////////////////////////////////////////////////////
  // Test bind for range query
  ///////////////////////////////////////////////////////////////////

  // Basic check to avoid surprise in later tests
  EXPECT_EQ(cl.size(), 1);
  EXPECT_EQ(cl[0].IsPointQuery(), false);
  EXPECT_EQ(isp.IsFullIndexScan(), false);

  // There are three unbound values
  EXPECT_EQ(cl[0].GetBindingCount(), 3);

  // At this time low key and high key should be binded
  // (Note that the type should be also INT but the value is likely to be 0)
  LOG_INFO("Low key (NOT BINDED) = %s", cl[0].GetLowKey()->GetInfo().c_str());
  LOG_INFO("High key (NOT BINDED) = %s", cl[0].GetHighKey()->GetInfo().c_str());

  // Bind real value
  common::Value val1 = (
      common::ValueFactory::GetIntegerValue(100).Copy());
  common::Value val2 = (
      common::ValueFactory::GetIntegerValue(200).Copy());
  common::Value val3 = (
      common::ValueFactory::GetIntegerValue(300).Copy());
  isp.LateBindValues(index_p, {val1, val2, val3});

  // This is important - Since binding does not change the number of
  // binding points, and their information is preserved for next
  // binding
  EXPECT_EQ(cl[0].GetBindingCount(), 3);

  // At this time low key and high key should be binded
  LOG_INFO("Low key = %s", cl[0].GetLowKey()->GetInfo().c_str());
  LOG_INFO("High key = %s", cl[0].GetHighKey()->GetInfo().c_str());

  ///////////////////////////////////////////////////////////////////
  // End of all tests
  ///////////////////////////////////////////////////////////////////
  delete index_p;

  return;
}

}  // End test namespace
}  // End peloton namespace
