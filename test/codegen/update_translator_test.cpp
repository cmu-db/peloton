//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator_test.cpp
//
// Identification: test/codegen/update_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "executor/update_executor.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of table
// scan query plans. All the tests use a single table created and loaded during
// SetUp().  The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 64 rows of random values.
//===----------------------------------------------------------------------===//

class UpdateTranslatorTest : public PelotonCodeGenTest {
 public:
  UpdateTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(10) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

 private:
  uint32_t num_rows_to_insert;
};

/**
 * @brief This test case uses the interpreted executor to perform an update.
 * This is an example showing what an update plan looks like.
 */
TEST_F(UpdateTranslatorTest, ToConstUpdate) {
  // UPDATE table
  // SET a = 1;

  storage::DataTable *table = &this->GetTestTable(this->TestTableId());
  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());

  // =============
  //  Create plan
  // =============

  // Each update query needs to first scan the table.
  std::unique_ptr<planner::SeqScanPlan> scan_plan(new planner::SeqScanPlan(
      &GetTestTable(TestTableId()), /* table */
      nullptr,                      /* predicate */
      {0, 1, 2, 3}                  /* columns */
  ));

  // Then it transforms each tuple scanned.
  // Weirdly, the transformation is represented by a "projection".
  std::unique_ptr<planner::ProjectInfo> project_info(new planner::ProjectInfo(
      // target list : [(oid_t, planner::DerivedAttribute)]
      // These specify columns that are transformed.
      {
          // Column 0 of the updated tuple will have constant value 1
          {
              0,
              planner::DerivedAttribute{
                  // I haven't figured out what this should be
                  planner::AttributeInfo{},

                  expression::ExpressionUtil::ConstantValueFactory(
                      type::ValueFactory::GetIntegerValue(1)
                  )
              }
          }
      },

      // direct map list : [(oid_t, (oid_t, oid_t))]
      // These specify columns that are directly pulled from the original tuple.
      {
          { 1, { 0, 1 } },
          { 2, { 0, 2 } },
          { 3, { 0, 3 } },
      }
  ));

  // Now embed the scan and the transformation to build up an update plan.
  std::unique_ptr<planner::UpdatePlan> update_plan(new planner::UpdatePlan(
      &GetTestTable(TestTableId()), /* table */
      std::move(project_info)       /* projection info */
  ));

  update_plan->AddChild(std::move(scan_plan));

  // ==============
  //  Execute plan
  // ==============

  auto &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn)
  );

  std::unique_ptr<executor::UpdateExecutor> update_executor =
      std::make_unique<executor::UpdateExecutor>(
          update_plan.get(), context.get()
      );

  std::unique_ptr<executor::SeqScanExecutor> scan_executor =
      std::make_unique<executor::SeqScanExecutor>(
          update_plan->GetChild(0), context.get()
      );

  update_executor->AddChild(scan_executor.get());

  EXPECT_TRUE(update_executor->Init());

  // The weird peloton behavior: each time we call Execute, only one tile group
  // is operated on.
  while (update_executor->Execute()) {}

  txn_manager.CommitTransaction(txn);

  LOG_DEBUG("Table has %zu tuples", table->GetTupleCount());
  LOG_INFO("%s", table->GetInfo().c_str());
}

}  // namespace test
}  // namespace peloton
