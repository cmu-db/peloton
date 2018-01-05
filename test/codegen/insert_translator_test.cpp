//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_translator_test.cpp
//
// Identification: test/codegen/insert_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"
#include "common/harness.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "sql/testing_sql_util.h"
#include "optimizer/optimizer.h"

namespace peloton {
namespace test {

class InsertTranslatorTest : public PelotonCodeGenTest {
 public:
  InsertTranslatorTest() : PelotonCodeGenTest() {}
  oid_t TestTableId1() { return test_table_oids[0]; }
  oid_t TestTableId2() { return test_table_oids[1]; }
};

// Insert one tuple
TEST_F(InsertTranslatorTest, InsertOneTuple) {
  // Check the pre-condition
  auto table = &GetTestTable(TestTableId1());
  auto num_tuples = table->GetTupleCount();
  EXPECT_EQ(num_tuples, 0);

  // Build an insert plan
  auto constant_expr_0 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(0));
  auto constant_expr_1 = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(1));
  auto constant_expr_2 = new expression::ConstantValueExpression(
      type::ValueFactory::GetDecimalValue(2));
  auto constant_expr_3 = new expression::ConstantValueExpression(
      type::ValueFactory::GetVarcharValue("Tuple1", true));
  std::vector<std::vector<ExpressionPtr>> tuples;
  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];
  values.push_back(ExpressionPtr(constant_expr_0));
  values.push_back(ExpressionPtr(constant_expr_1));
  values.push_back(ExpressionPtr(constant_expr_2));
  values.push_back(ExpressionPtr(constant_expr_3));

  std::vector<std::string> columns;
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table, &columns, &tuples));

  // Bind the plan
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // Prepare a consumer to collect the result
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(*insert_plan, buffer);

  // Check the post-condition, i.e. verify the result
  num_tuples = table->GetTupleCount();
  EXPECT_EQ(num_tuples, 1);

  // Setup the scan plan node
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_table1(
      new planner::SeqScanPlan(table, nullptr, {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context1;
  seq_scan_plan_table1->PerformBinding(context1);

  // Printing consumer
  codegen::BufferingConsumer buffer_table1{{0, 1, 2, 3}, context1};

  // COMPILE and execute
  CompileAndExecute(*seq_scan_plan_table1, buffer_table1);

  // Check that we got all the results
  auto &results_table1 = buffer_table1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetDecimalValue(2)));
  EXPECT_EQ(CmpBool::TRUE,
            results_table1[0].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("Tuple1")));
}

// Insert all tuples from table2 into table1.
TEST_F(InsertTranslatorTest, InsertScanTranslator) {
  auto table1 = &GetTestTable(TestTableId1());
  auto table2 = &GetTestTable(TestTableId2());

  LoadTestTable(TestTableId2(), 10);

  // Insert plan for table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1));

  // Scan plan for table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, {0, 1, 2, 3}));

  insert_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // Collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*insert_plan, buffer);
  auto &results = buffer.GetOutputTuples();
  (void)results;

  EXPECT_EQ(table1->GetTupleCount(), table2->GetTupleCount());

  // Setup the scan plan node
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_table1(
      new planner::SeqScanPlan(table1, nullptr, {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context1;
  seq_scan_plan_table1->PerformBinding(context1);

  // Printing consumer
  codegen::BufferingConsumer buffer_table1{{0, 1, 2, 3}, context1};

  // COMPILE and execute
  CompileAndExecute(*seq_scan_plan_table1, buffer_table1);

  // Check that we got all the results
  auto &results_table1 = buffer_table1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(2)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(3).CompareEquals(
                                     type::ValueFactory::GetVarcharValue("3")));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(90)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(91)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(92)));
  EXPECT_EQ(CmpBool::TRUE,
            results_table1[9].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("93")));
}

// Insert all tuples from table2 into table1 with null values.
TEST_F(InsertTranslatorTest, InsertScanTranslatorWithNull) {
  auto table1 = &GetTestTable(TestTableId1());
  auto table2 = &GetTestTable(TestTableId2());

  const bool insert_nulls = true;
  LoadTestTable(TestTableId2(), 10, insert_nulls);

  // Insert plan for table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1));

  // Scan plan for table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, {0, 1, 2, 3}));

  insert_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // Collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*insert_plan, buffer);
  auto &results = buffer.GetOutputTuples();
  (void)results;

  EXPECT_EQ(table1->GetTupleCount(), table2->GetTupleCount());

  // Setup the scan plan node
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_table1(
      new planner::SeqScanPlan(table1, nullptr, {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context1;
  seq_scan_plan_table1->PerformBinding(context1);

  // Printing consumer
  codegen::BufferingConsumer buffer_table1{{0, 1, 2, 3}, context1};

  // COMPILE and execute
  CompileAndExecute(*seq_scan_plan_table1, buffer_table1);

  // Check that we got all the results
  auto &results_table1 = buffer_table1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_TRUE(results_table1[0].GetValue(1).IsNull());
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(2)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(3).CompareEquals(
                                     type::ValueFactory::GetVarcharValue("3")));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(90)));
  EXPECT_TRUE(results_table1[9].GetValue(1).IsNull());
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(92)));
  EXPECT_EQ(CmpBool::TRUE,
            results_table1[9].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("93")));
}

// Insert a tuple from table2 with column order changed, into table1.
TEST_F(InsertTranslatorTest, InsertScanColumnTranslator) {
  auto table1 = &GetTestTable(TestTableId1());
  auto table2 = &GetTestTable(TestTableId2());

  LoadTestTable(TestTableId2(), 10);

  // Insert plan for table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1));

  // Scan plan for table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, {1, 0, 2, 3}));

  insert_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // Collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*insert_plan, buffer);
  auto &results = buffer.GetOutputTuples();
  (void)results;

  EXPECT_EQ(table1->GetTupleCount(), table2->GetTupleCount());

  // Setup the scan plan node
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan_table1(
      new planner::SeqScanPlan(table1, nullptr, {0, 1, 2, 3}));

  // Do binding
  planner::BindingContext context1;
  seq_scan_plan_table1->PerformBinding(context1);

  // Printing consumer
  codegen::BufferingConsumer buffer_table1{{0, 1, 2, 3}, context1};

  // COMPILE and execute
  CompileAndExecute(*seq_scan_plan_table1, buffer_table1);

  // Check that we got all the results
  auto &results_table1 = buffer_table1.GetOutputTuples();

  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(2)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[0].GetValue(3).CompareEquals(
                                     type::ValueFactory::GetVarcharValue("3")));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(91)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(90)));
  EXPECT_EQ(CmpBool::TRUE, results_table1[9].GetValue(2).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(92)));
  EXPECT_EQ(CmpBool::TRUE,
            results_table1[9].GetValue(3).CompareEquals(
                type::ValueFactory::GetVarcharValue("93")));
}

}  // namespace test
}  // namespace peloton
