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
#include "common/harness.h"
#include "codegen/insert/insert_tuples_translator.h"
#include "codegen/codegen_test_util.h"

#define NUM_OF_INSERT_ROWS 100000

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
  InsertTranslatorTest() : PelotonCodeGenTest() {
    LoadTestTable(TestTable4Id(), NUM_OF_INSERT_ROWS);
  }

  uint32_t TestTable1Id() { return test_table1_id; }

  uint32_t TestTable2Id() { return test_table2_id; }

  uint32_t TestTable3Id() { return test_table3_id; }

  uint32_t TestTable4Id() { return test_table4_id; }

  void clearTable3() {
    std::unique_ptr<planner::DeletePlan> delete_plan{
        new planner::DeletePlan(&GetTestTable(TestTable3Id()), nullptr)};
    std::unique_ptr<planner::AbstractPlan> scan{
        new planner::SeqScanPlan(&GetTestTable(TestTable3Id()), nullptr, {0, 1, 2})};
    delete_plan->AddChild(std::move(scan));

    planner::BindingContext deleteContext;
    delete_plan->PerformBinding(deleteContext);

    codegen::BufferingConsumer buffer{{0, 1}, deleteContext};
    CompileAndExecute(*delete_plan, buffer, reinterpret_cast<char*>(buffer.GetState()));
  }
};


//TEST_F(InsertTranslatorTest, InsertTuples) {
//  auto table = &this->GetTestTable(this->TestTable1Id());
//  auto table2 = &this->GetTestTable(this->TestTable2Id());
//
//  LOG_INFO("Before Insert: #tuples in table = %zu", table->GetTupleCount());
//
//  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
//
//  Timer<std::ratio<1, 1000>> timer;
//  double compileTotalTime = 0;
//  double executeTotalTime = 0;
//
//  for (int i = 0; i < NUM_OF_INSERT_ROWS; i++) {
//
//    if (i % 500 == 0) {
//      LOG_INFO("Current i: %d", i);
//    }
//    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(), true));
//    tuple->SetValue(0, type::ValueFactory::GetIntegerValue(10 * i));
//    tuple->SetValue(1, type::ValueFactory::GetIntegerValue(10 * i + 1));
//    tuple->SetValue(2, type::ValueFactory::GetIntegerValue(10 * i + 2));
//    tuple->SetValue(3, type::ValueFactory::GetVarcharValue("hello world", true), testing_pool);
//
//    std::unique_ptr<planner::InsertPlan> insert_plan(new planner::InsertPlan(table, std::move(tuple)));
//
//    planner::BindingContext context;
//    insert_plan->PerformBinding(context);
//    codegen::BufferingConsumer buffer{{0, 1}, context};
//
//    timer.Start();
//    CompileAndExecute(*insert_plan, buffer,
//                      reinterpret_cast<char *>(buffer.GetState()));
//    timer.Stop();
//    compileTotalTime += timer.GetDuration();
//    timer.Reset();
//
//    std::unique_ptr<storage::Tuple> tuple2(new storage::Tuple(table2->GetSchema(), true));
//    tuple2->SetValue(0, type::ValueFactory::GetIntegerValue(10 * i + 5));
//    tuple2->SetValue(1, type::ValueFactory::GetIntegerValue(10 * i + 6));
//    tuple2->SetValue(2, type::ValueFactory::GetIntegerValue(10 * i + 7));
//    tuple2->SetValue(3, type::ValueFactory::GetVarcharValue("Advanced DB", true), testing_pool);
//
//    std::unique_ptr<planner::InsertPlan> insert_plan_2(new planner::InsertPlan(table2, std::move(tuple2)));
//    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    concurrency::Transaction *txn = txn_manager.BeginTransaction();
//    std::unique_ptr<executor::ExecutorContext> executor_context(
//        new executor::ExecutorContext(txn)
//    );
//
//    std::unique_ptr<executor::InsertExecutor> insert_executor =
//        std::make_unique<executor::InsertExecutor>(
//            insert_plan_2.get(), executor_context.get()
//        );
//
//    timer.Start();
//    while (insert_executor->Execute()) {}
//    txn_manager.CommitTransaction(txn);
//    timer.Stop();
//    executeTotalTime += timer.GetDuration();
//    timer.Reset();
//  }
//  ASSERT_EQ(table->GetTupleCount(), NUM_OF_INSERT_ROWS);
//  ASSERT_EQ(table2->GetTupleCount(), NUM_OF_INSERT_ROWS);
//  LOG_INFO("compiled insert finished in: %.2f ms\n", compileTotalTime);
//  LOG_INFO("executor insert finished in: %.2f ms\n", executeTotalTime);
//}

/**
 * @brief Insert all tuples in table4 into table3.
 *
 * This test uses the interpreted executor, just for comparison.
 */
TEST_F(InsertTranslatorTest, InsertScanExecutor) {
  auto table3 = &this->GetTestTable(this->TestTable3Id());
  auto table4 = &this->GetTestTable(this->TestTable4Id());

  // Insert into table3
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table3)
  );

  // Scan from table2
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table4, nullptr, { 0, 1, 2, 3 })
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

  LOG_DEBUG("Table 3 has %zu tuples", table3->GetTupleCount());
  LOG_DEBUG("Table 4 has %zu tuples", table4->GetTupleCount());
}

/**
 * @brief Insert all tuples in table4 into table3, but use codegen
 */
TEST_F(InsertTranslatorTest, InsertScanTranslator) {

//  clearTable3();
  auto table3 = &this->GetTestTable(this->TestTable3Id());
  auto table4 = &this->GetTestTable(this->TestTable4Id());

  LOG_DEBUG("Table 3 has %zu tuples", table3->GetTupleCount());
  LOG_DEBUG("Table 4 has %zu tuples", table4->GetTupleCount());

  // Insert into table3
  std::unique_ptr<planner::InsertPlan> insert_plan(
      new planner::InsertPlan(table3)
  );

  // Scan from table4
  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(table4, nullptr, { 0, 1, 2, 3 })
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

  PL_ASSERT(table3->GetTupleCount() == table4->GetTupleCount());
  LOG_DEBUG("Table 3 has %zu tuples", table3->GetTupleCount());
  LOG_DEBUG("Table 4 has %zu tuples", table4->GetTupleCount());
}

}  // namespace test
}  // namespace peloton
