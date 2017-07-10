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
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace test {

class InsertTranslatorTest : public PelotonCodeGenTest {
 public:
  InsertTranslatorTest() : PelotonCodeGenTest() {}
  TableId TestTableId1() { return TableId::_1; }
  TableId TestTableId2() { return TableId::_2; }
};

// Insert one tuple
TEST_F(InsertTranslatorTest, InsertOneTuple) {

  // Check the pre-condition
  auto table = &GetTestTable(TestTableId1());
  auto num_tuples = table->GetTupleCount();
  EXPECT_EQ(num_tuples, 0);

  // Build an insert plan
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(),
                                        true));
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(0));
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(1));
  tuple->SetValue(2, type::ValueFactory::GetIntegerValue(2));
  tuple->SetValue(3, type::ValueFactory::GetVarcharValue("Tuple1", true), pool);

  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table, std::move(tuple)));

  // Bind the plan
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // Prepare a consumer to collect the result
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // Compile and execute
  CompileAndExecute(*insert_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  // Check the post-condition, i.e. verify the result
  num_tuples = table->GetTupleCount();
  EXPECT_EQ(num_tuples, 1);
}

// Insert all tuples from table2 into table1.
// This test uses the interpreted executor, just for comparison.
TEST_F(InsertTranslatorTest, InsertScanExecutor) {
  auto table1 = &GetTestTable(TestTableId1());
  auto table2 = &GetTestTable(TestTableId2());

  LoadTestTable(TestTableId2(), 10);

  // Insert into table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1));

  // Scan from table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, {0, 1, 2, 3}));

  insert_plan->AddChild(std::move(seq_scan_plan));

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::unique_ptr<executor::InsertExecutor> insert_executor =
      std::make_unique<executor::InsertExecutor>(insert_plan.get(),
                                                 context.get());

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(insert_plan->GetChild(0),
                                                  context.get());

  insert_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(insert_executor->Init());
  EXPECT_TRUE(insert_executor->Execute());

  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(table1->GetTupleCount(), table2->GetTupleCount());
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
  CompileAndExecute(*insert_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));
  auto &results = buffer.GetOutputTuples();
  (void)results;

  EXPECT_EQ(table1->GetTupleCount(), table2->GetTupleCount());
}

}  // namespace test
}  // namespace peloton
