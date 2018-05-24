//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_translator_test.cpp
//
// Identification: test/codegen/limit_translator_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/limit_plan.h"
#include "planner/seq_scan_plan.h"
#include "storage/storage_manager.h"
#include "storage/table_factory.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class LimitTranslatorTest : public PelotonCodeGenTest {
 public:
  LimitTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  oid_t TestTableId() { return test_table_oids[0]; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(LimitTranslatorTest, OffsetLimitScan) {
  // Test 1:
  //
  // SELECT * FROM table offset 0 limit 1;
  //
  // Should return exactly one row

  {
    uint32_t offset = 0;
    uint32_t limit = 1;

    LOG_INFO("SELECT * from test_table offset %u limit %u", offset, limit);

    // Setup the scan->limit plan
    std::unique_ptr<planner::AbstractPlan> scan_plan{new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
    std::unique_ptr<planner::AbstractPlan> limit_plan{
        new planner::LimitPlan(limit, offset)};
    limit_plan->AddChild(std::move(scan_plan));

    // Do binding
    planner::BindingContext context;
    limit_plan->PerformBinding(context);

    // Printing consumer
    codegen::BufferingConsumer buffer{{0, 1, 2}, context};

    // COMPILE and execute
    CompileAndExecute(*limit_plan, buffer);

    // Check that we got all the results
    const auto &results = buffer.GetOutputTuples();
    EXPECT_EQ(limit, results.size());
  }

  // Test 2:
  //
  // SELECT * FROM table offset (table_size / 2) limit (table_size / 2);
  //
  // Should return exactly one row

  {
    uint32_t offset = NumRowsInTestTable() / 2;
    uint32_t limit = offset;

    LOG_INFO("SELECT * from test_table offset %u limit %u", offset, limit);

    // Setup the scan->limit plan
    std::unique_ptr<planner::AbstractPlan> scan_plan{new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
    std::unique_ptr<planner::AbstractPlan> limit_plan{
        new planner::LimitPlan(limit, offset)};
    limit_plan->AddChild(std::move(scan_plan));

    // Do binding
    planner::BindingContext context;
    limit_plan->PerformBinding(context);

    // Printing consumer
    codegen::BufferingConsumer buffer{{0, 1, 2}, context};

    // COMPILE and execute
    CompileAndExecute(*limit_plan, buffer);

    // Check that we got all the results
    const auto &results = buffer.GetOutputTuples();
    EXPECT_EQ(limit, results.size());
  }

  // Test 3:
  //
  // SELECT * FROM table offset (table_size / 2) limit 4;
  //
  // Should return exactly one row

  {
    uint32_t offset = NumRowsInTestTable() / 2;
    uint32_t limit = 4;

    LOG_INFO("SELECT * from test_table offset %u limit %u", offset, limit);

    // Setup the scan->limit plan
    std::unique_ptr<planner::AbstractPlan> scan_plan{new planner::SeqScanPlan(
        &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
    std::unique_ptr<planner::AbstractPlan> limit_plan{
        new planner::LimitPlan(limit, offset)};
    limit_plan->AddChild(std::move(scan_plan));

    // Do binding
    planner::BindingContext context;
    limit_plan->PerformBinding(context);

    // Printing consumer
    codegen::BufferingConsumer buffer{{0, 1, 2}, context};

    // COMPILE and execute
    CompileAndExecute(*limit_plan, buffer);

    // Check that we got all the results
    const auto &results = buffer.GetOutputTuples();
    EXPECT_EQ(limit, results.size());
  }
}

TEST_F(LimitTranslatorTest, SkipAll) {
  //
  // SELECT * FROM table offset (table_size + 1) limit 1;
  //

  uint32_t offset = NumRowsInTestTable() + 1;
  uint32_t limit = 1;

  LOG_INFO("SELECT * from test_table offset %u limit %u", offset, limit);

  // Setup the scan->limit plan
  std::unique_ptr<planner::AbstractPlan> scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  std::unique_ptr<planner::AbstractPlan> limit_plan{
      new planner::LimitPlan(limit, offset)};
  limit_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  limit_plan->PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*limit_plan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(0, results.size());
}

TEST_F(LimitTranslatorTest, ReturnLast) {
  //
  // SELECT * FROM table offset (table_size - 1) limit (table_size);
  //

  uint32_t offset = NumRowsInTestTable() - 1;
  uint32_t limit = NumRowsInTestTable();

  LOG_INFO("SELECT * from test_table offset %u limit %u", offset, limit);

  // Setup the scan->limit plan
  std::unique_ptr<planner::AbstractPlan> scan_plan{new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), nullptr, {0, 1, 2})};
  std::unique_ptr<planner::AbstractPlan> limit_plan{
      new planner::LimitPlan(limit, offset)};
  limit_plan->AddChild(std::move(scan_plan));

  // Do binding
  planner::BindingContext context;
  limit_plan->PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*limit_plan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

}  // namespace test
}  // namespace peloton
