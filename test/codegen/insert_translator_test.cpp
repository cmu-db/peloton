//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker_proxy.h
//
// Identification: src/include/codegen/value_peeker_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/plan_executor.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "common/harness.h"
#include "parser/insert_statement.h"
#include "planner/insert_plan.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of insert
// plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
//===----------------------------------------------------------------------===//

class InsertTranslatorTest : public PelotonCodeGenTest {
 public:
  InsertTranslatorTest() : PelotonCodeGenTest() {}

  uint32_t TestTable1Id() { return test_table1_id; }

  uint32_t TestTable2Id() { return test_table2_id; }
};

TEST_F(InsertTranslatorTest, InsertTuples) {
  auto table = &this->GetTestTable(this->TestTable1Id());

  LOG_DEBUG("Before Insert: #tuples in table = %zu", table->GetTupleCount());

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(), true));
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(10));
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(11));
  tuple->SetValue(2, type::ValueFactory::GetIntegerValue(12));
  tuple->SetValue(3, type::ValueFactory::GetVarcharValue("he", true), testing_pool);

  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table, std::move(tuple)));

  // Do binding
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*insert_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  size_t num_tuples = table->GetTupleCount();
  LOG_DEBUG("After Insert: #tuples in table = %zu", num_tuples);
  ASSERT_EQ(num_tuples, 1);
}

TEST_F(InsertTranslatorTest, InsertScanExecutor) {
  auto table1 = &this->GetTestTable(this->TestTable1Id());
  auto table2 = &this->GetTestTable(this->TestTable2Id());

//  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
//  (void)testing_pool;

  this->LoadTestTable(this->TestTable2Id(), 10);

  // Insert into table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1)
  );

  // Scan from table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, { 0, 1, 2, 3 })
  );

  insert_plan->AddChild(std::move(seq_scan_plan));

  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::InsertExecutor> insert_executor =
      std::make_unique<executor::InsertExecutor>(
          insert_plan.get(), context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          insert_plan->GetChild(0), context.get()
      );

  insert_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(insert_executor->Init());
  EXPECT_TRUE(insert_executor->Execute());

  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Table 2 has %zu tuples", table2->GetTupleCount());
  LOG_DEBUG("Table 1 has %zu tuples", table1->GetTupleCount());
}

TEST_F(InsertTranslatorTest, InsertScanTranslator) {
  auto table1 = &this->GetTestTable(this->TestTable1Id());
  auto table2 = &this->GetTestTable(this->TestTable2Id());

//  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
//  (void)testing_pool;

  this->LoadTestTable(this->TestTable2Id(), 10);

  // Insert into table1
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table1)
  );

  // Scan from table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table2, nullptr, { 0, 1, 2, 3 })
  );

  insert_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  insert_plan->PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*insert_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  (void)results;

  PL_ASSERT(table1->GetTupleCount() == table2->GetTupleCount());
}

}  // namespace test
}  // namespace peloton
