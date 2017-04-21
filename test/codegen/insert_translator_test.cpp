//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
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

  uint32_t TestTableId() { return test_table1_id; }
};

TEST_F(InsertTranslatorTest, InsertTuples) {
  auto table = &this->GetTestTable(this->TestTableId());

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

}  // namespace test
}  // namespace peloton
