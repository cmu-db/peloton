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
#include "planner/delete_plan.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of order-by
// (sort) plans. All tests use a test table with the following schema:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// Each test may choose to sort on a different column.
//===----------------------------------------------------------------------===//

class DeleteTranslatorTest : public PelotonCodeGenTest {
 public:
  DeleteTranslatorTest() : PelotonCodeGenTest() {}

  uint32_t TestTableId() { return test_table1_id; }
};

TEST_F(DeleteTranslatorTest, SimpleDeleteTest) {
  // Load table with 20 rows
  uint32_t num_test_rows = 20;
  LoadTestTable(TestTableId(), num_test_rows);

  std::unique_ptr<planner::DeletePlan> delete_plan(
      new planner::DeletePlan(
          &GetTestTable(TestTableId()),
          nullptr
      )
  );

  std::unique_ptr<planner::SeqScanPlan> seq_scan_plan(
      new planner::SeqScanPlan(
          &GetTestTable(TestTableId()),
          nullptr,
          {0, 1, 2, 3}
      )
  );

  delete_plan->AddChild(std::move(seq_scan_plan));

  // Do binding
  planner::BindingContext context;
  delete_plan->PerformBinding(context);


  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(*delete_plan, buffer,
                    reinterpret_cast<char *>(buffer.GetState()));

  // The results should be sorted in ascending order
  auto &results = buffer.GetOutputTuples();
  (void)results;
//  EXPECT_EQ(results.size(), num_test_rows);
//  EXPECT_TRUE(std::is_sorted(
//      results.begin(), results.end(),
//      [](const codegen::WrappedTuple &t1, const codegen::WrappedTuple &t2) {
//        auto is_lte = t1.GetValue(0).CompareLessThanEquals(t2.GetValue(0));
//        return is_lte == type::CMP_TRUE;
//      }));
}

}
}
