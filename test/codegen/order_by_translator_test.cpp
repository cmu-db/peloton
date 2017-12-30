//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator_test.cpp
//
// Identification: test/codegen/order_by_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class OrderByTranslatorTest : public PelotonCodeGenTest {
 public:
  OrderByTranslatorTest() : PelotonCodeGenTest() {}

  oid_t TestTableId() { return test_table_oids[0]; }
};

TEST_F(OrderByTranslatorTest, SingleIntColAscTest) {
  //
  // SELECT * FROM test_table ORDER BY a;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(TestTableId(), num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({0}, {false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*order_by_plan, buffer);

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        auto is_lte = t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0));
        return is_lte == CmpBool::TRUE;
      }));
}

TEST_F(OrderByTranslatorTest, SingleIntColDescTest) {
  //
  // SELECT * FROM test_table ORDER BY a DESC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(TestTableId(), num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({0}, {true}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*order_by_plan, buffer);

  // The results should be sorted in descending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);
  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        auto is_gte = t1.GetValue(0).CompareGreaterThanEquals(t2.GetValue(0));
        return is_gte == CmpBool::TRUE;
      }));
}

TEST_F(OrderByTranslatorTest, MultiIntColAscTest) {
  //
  // SELECT * FROM test_table ORDER BY b, a ASC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(TestTableId(), num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {false, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*order_by_plan, buffer);

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        if (t1.GetValue(1).CompareEquals(t2.GetValue(0)) ==
            CmpBool::TRUE) {
          // t1.b == t2.b => t1.a <= t2.a
          return t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0)) ==
                 CmpBool::TRUE;
        } else {
          // t1.b != t2.b => t1.b < t2.b
          return t1.GetValue(1).CompareLessThan(t2.GetValue(1)) ==
                 CmpBool::TRUE;
        }
      }));
}

TEST_F(OrderByTranslatorTest, MultiIntColMixedTest) {
  //
  // SELECT * FROM test_table ORDER BY b DESC a ASC;
  //

  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(TestTableId(), num_test_rows);

  std::unique_ptr<planner::OrderByPlan> order_by_plan{
      new planner::OrderByPlan({1, 0}, {true, false}, {0, 1, 2, 3})};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2, 3})};

  order_by_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  order_by_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*order_by_plan, buffer);

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), num_test_rows);

  EXPECT_TRUE(std::is_sorted(
      results.begin(), results.end(),
      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
        if (t1.GetValue(1).CompareEquals(t2.GetValue(1)) ==
            CmpBool::TRUE) {
          // t1.b == t2.b => t1.a <= t2.a
          return t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0)) ==
                 CmpBool::TRUE;
        } else {
          // t1.b != t2.b => t1.b > t2.b
          return t1.GetValue(1).CompareGreaterThan(t2.GetValue(1)) ==
                 CmpBool::TRUE;
        }
      }));
}

}  // namespace test
}  // namespace peloton