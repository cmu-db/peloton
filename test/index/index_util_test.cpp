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

  catalog::Column column0(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "A",
                          true);

  catalog::Column column1(type::TypeId::VARCHAR, 1024, "B", false);

  // The following twoc constitutes tuple schema but does not appear in index

  catalog::Column column2(type::TypeId::DECIMAL,
                          type::Type::GetTypeSize(type::TypeId::DECIMAL), "C",
                          true);

  catalog::Column column3(type::TypeId::INTEGER,
                          type::Type::GetTypeSize(type::TypeId::INTEGER), "D",
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
      INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::DEFAULT, tuple_schema.get(), key_schema, key_attrs,
      true);  // unique_keys

  // Build index
  index::Index *index = index::IndexFactory::GetIndex(index_metadata);

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
  std::unique_ptr<index::Index> index_p(BuildIndex());
  bool ret;

  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  // Test basic

  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {3, 0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_TRUE(ret);
  value_index_list.clear();

  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {1, 0, 3},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_TRUE(ret);
  value_index_list.clear();

  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {0, 1, 3},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_TRUE(ret);
  value_index_list.clear();

  // Test whether reconizes if only two columns are matched

  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {0, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {3, 0},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  // Test empty

  ret = IndexUtil::FindValueIndex(index_p->GetMetadata(), {}, {}, value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  // Test redundant conditions

  // This should return false, since the < already defines a lower bound
  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {0, 3, 3, 0, 3, 1},
      {ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  // This should return true
  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {0, 3, 3, 0, 3, 1},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_LESSTHAN,
       ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_TRUE(ret);
  value_index_list.clear();

  // Test duplicated conditions on a single column
  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {3, 3, 3},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_EQUAL},
      value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  // The last test should logically be classified as point query
  // but our procedure does not give positive result to reduce
  // the complexity
  ret = IndexUtil::FindValueIndex(
      index_p->GetMetadata(), {3, 0, 1, 0},
      {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHANOREQUALTO,
       ExpressionType::COMPARE_EQUAL,
       ExpressionType::COMPARE_GREATERTHANOREQUALTO},
      value_index_list);
  EXPECT_FALSE(ret);
  value_index_list.clear();

  return;
}

/*
 * ConstructBoundaryKeyTest() - Tests ConstructBoundaryKey() function for
 *                              conjunctions
 */
TEST_F(IndexUtilTests, ConstructBoundaryKeyTest) {
  std::unique_ptr<index::Index> index_p(BuildIndex());

  // This is the output variable
  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  std::vector<type::Value> value_list{};
  std::vector<oid_t> tuple_column_id_list{};
  std::vector<ExpressionType> expr_list{};

  value_list = {
      type::ValueFactory::GetIntegerValue(100).Copy(),
      type::ValueFactory::GetIntegerValue(200).Copy(),
      type::ValueFactory::GetIntegerValue(50).Copy(),
  };

  tuple_column_id_list = {3, 3, 0};

  expr_list = {
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO,
  };

  IndexScanPredicate isp{};

  isp.AddConjunctionScanPredicate(index_p.get(), value_list,
                                  tuple_column_id_list, expr_list);

  const std::vector<ConjunctionScanPredicate> &cl = isp.GetConjunctionList();

  // First check the conjunction has been pushed into the scan predicate object
  EXPECT_EQ(cl.size(), 1UL);

  // Then make sure all values have been bound (i.e. no free variable)
  EXPECT_EQ(cl[0].GetBindingCount(), 0UL);

  // Check whether the entire predicate is full index scan (should not be)
  EXPECT_FALSE(isp.IsFullIndexScan());

  // Then check the conjunction predicate
  EXPECT_FALSE(cl[0].IsFullIndexScan());

  // Then check whether the conjunction predicate is a point query
  EXPECT_FALSE(cl[0].IsPointQuery());

  LOG_INFO("Low key = %s", cl[0].GetLowKey()->GetInfo().c_str());
  LOG_INFO("High key = %s", cl[0].GetHighKey()->GetInfo().c_str());

  ///////////////////////////////////////////////////////////////////
  // Test the case where there is no optimization that could be done
  //
  // The condition means first index column does not equal 100
  ///////////////////////////////////////////////////////////////////

  value_list = {
      type::ValueFactory::GetIntegerValue(100).Copy(),
  };

  tuple_column_id_list = {
      3,
  };

  expr_list = {
      ExpressionType::COMPARE_NOTEQUAL,
  };

  isp.AddConjunctionScanPredicate(index_p.get(), value_list,
                                  tuple_column_id_list, expr_list);

  const std::vector<ConjunctionScanPredicate> &cl2 = isp.GetConjunctionList();

  EXPECT_EQ(cl2.size(), 2UL);
  EXPECT_EQ(cl2[1].GetBindingCount(), 0UL);
  EXPECT_TRUE(isp.IsFullIndexScan());
  EXPECT_TRUE(cl2[1].IsFullIndexScan());
  EXPECT_FALSE(cl2[1].IsPointQuery());

  ///////////////////////////////////////////////////////////////////
  // Test point query and query key
  //
  // Index key = <100, 50, "Peloton!">
  ///////////////////////////////////////////////////////////////////

  IndexScanPredicate isp2{};

  value_list = {
      type::ValueFactory::GetIntegerValue(100).Copy(),
      type::ValueFactory::GetVarcharValue("Peloton!").Copy(),
      type::ValueFactory::GetIntegerValue(50).Copy(),
  };

  tuple_column_id_list = {3, 1, 0};

  expr_list = {
      ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_EQUAL,
      ExpressionType::COMPARE_EQUAL,
  };

  isp2.AddConjunctionScanPredicate(index_p.get(), value_list,
                                   tuple_column_id_list, expr_list);

  const std::vector<ConjunctionScanPredicate> &cl3 = isp2.GetConjunctionList();

  EXPECT_EQ(cl3.size(), 1UL);
  EXPECT_EQ(cl3[0].GetBindingCount(), 0UL);
  EXPECT_FALSE(isp2.IsFullIndexScan());
  EXPECT_FALSE(cl3[0].IsFullIndexScan());
  EXPECT_TRUE(cl3[0].IsPointQuery());

  LOG_INFO("Point query key = %s",
           cl3[0].GetPointQueryKey()->GetInfo().c_str());

  ///////////////////////////////////////////////////////////////////
  // End of all test cases
  ///////////////////////////////////////////////////////////////////

  return;
}

/*
 * BindKeyTest() - Tests binding values onto keys that are not bound
 */
TEST_F(IndexUtilTests, BindKeyTest) {
  std::unique_ptr<index::Index> index_p(BuildIndex());

  // This is the output variable
  std::vector<std::pair<oid_t, oid_t>> value_index_list{};

  std::vector<type::Value> value_list{};
  std::vector<oid_t> tuple_column_id_list{};
  std::vector<ExpressionType> expr_list{};

  value_list = {
      type::ValueFactory::GetParameterOffsetValue(2).Copy(),
      type::ValueFactory::GetParameterOffsetValue(0).Copy(),
      type::ValueFactory::GetParameterOffsetValue(1).Copy(),
  };

  tuple_column_id_list = {3, 3, 0};

  expr_list = {
      ExpressionType::COMPARE_GREATERTHAN,
      ExpressionType::COMPARE_LESSTHANOREQUALTO,
      ExpressionType::COMPARE_GREATERTHANOREQUALTO,
  };

  IndexScanPredicate isp{};

  isp.AddConjunctionScanPredicate(index_p.get(), value_list,
                                  tuple_column_id_list, expr_list);

  const std::vector<ConjunctionScanPredicate> &cl = isp.GetConjunctionList();

  ///////////////////////////////////////////////////////////////////
  // Test bind for range query
  ///////////////////////////////////////////////////////////////////

  // Basic check to avoid surprise in later tests
  EXPECT_EQ(cl.size(), 1);
  EXPECT_FALSE(cl[0].IsPointQuery());
  EXPECT_FALSE(isp.IsFullIndexScan());

  // There are three unbound values
  EXPECT_EQ(cl[0].GetBindingCount(), 3);

  // At this time low key and high key should be binded
  // (Note that the type should be also INT but the value is likely to be 0)
  LOG_INFO("Low key (NOT BINDED) = %s", cl[0].GetLowKey()->GetInfo().c_str());
  LOG_INFO("High key (NOT BINDED) = %s", cl[0].GetHighKey()->GetInfo().c_str());

  // Bind real value
  type::Value val1 = (
      type::ValueFactory::GetIntegerValue(100).Copy());
  type::Value val2 = (
      type::ValueFactory::GetIntegerValue(200).Copy());
  type::Value val3 = (
      type::ValueFactory::GetIntegerValue(300).Copy());
  isp.LateBindValues(index_p.get(), {val1, val2, val3});

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

  return;
}

}  // namespace test
}  // namespace peloton
