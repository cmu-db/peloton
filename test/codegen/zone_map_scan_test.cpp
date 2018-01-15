//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_scan_test.cpp
//
// Identification: test/codegen/zone_map_scan_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/storage_manager.h"
#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/seq_scan_plan.h"
#include "codegen/counting_consumer.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class ZoneMapScanTest : public PelotonCodeGenTest {
  std::string all_cols_table_name = "skipping_table";

 public:
  ZoneMapScanTest()
      : PelotonCodeGenTest(TEST_TUPLES_PER_TILEGROUP), num_rows_to_insert(20) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);

    MakeImmutableAndCreateZoneMaps(TestTableId());
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  oid_t TestTableId() { return test_table_oids[0]; }

  void MakeImmutableAndCreateZoneMaps(oid_t table_id) {
    auto &table = GetTestTable(table_id);
    oid_t num_tile_groups = table.GetTileGroupCount();
    // First Make Immutable.
    for (oid_t i = 0; i < num_tile_groups - 1; i++) {
      auto tile_group = table.GetTileGroup(i);
      auto tile_group_ptr = tile_group.get();
      auto tile_group_header = tile_group_ptr->GetHeader();
      tile_group_header->SetImmutability();
    }
    // Create Zone Maps.
    auto catalog = catalog::Catalog::GetInstance();
    (void)catalog;
    storage::ZoneMapManager *zone_map_manager =
        storage::ZoneMapManager::GetInstance();
    zone_map_manager->CreateZoneMapTableInCatalog();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    zone_map_manager->CreateZoneMapsForTable(&table, txn);
    txn_manager.CommitTransaction(txn);
  }

 private:
  uint32_t num_rows_to_insert = 100;
};

TEST_F(ZoneMapScanTest, ScanNoPredicates) {
  // SELECT a, b, c FROM table;
  // 1) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1, 2}};
  // 2) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};
  // COMPILE and execute
  CompileAndExecute(scan, buffer);
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(ZoneMapScanTest, SimplePredicate) {
  // SELECT a, b, c FROM table where a >= 20;
  // 1) Setup the predicate
  ExpressionPtr a_gt_20 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));
  // 2) Setup the scan plan node
  auto &table = GetTestTable(TestTableId());
  planner::SeqScanPlan scan{&table, a_gt_20.release(), {0, 1, 2}};
  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};
  // COMPILE and execute
  CompileAndExecute(scan, buffer);
  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 2, results.size());
}

TEST_F(ZoneMapScanTest, PredicateOnNonOutputColumn) {
  // SELECT b FROM table where a >= 40;
  // 1) Setup the predicate
  ExpressionPtr a_gt_40 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(40));
  // 2) Setup the scan plan node
  auto &table = GetTestTable(TestTableId());
  planner::SeqScanPlan scan{&table, a_gt_40.release(), {0, 1}};
  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0}, context};
  // COMPILE and execute
  CompileAndExecute(scan, buffer);
  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable() - 4, results.size());
}

TEST_F(ZoneMapScanTest, ScanwithConjunctionPredicate) {
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  // 1) Construct the components of the predicate
  // a >= 20
  ExpressionPtr a_gt_20 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));
  // b = 21
  ExpressionPtr b_eq_21 =
      CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(21));
  // a >= 20 AND b = 21
  auto *conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21.release(), a_gt_20.release());
  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), conj_eq, {0, 1, 2}};
  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);
  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};
  // COMPILE and execute
  CompileAndExecute(scan, buffer);
  // Check output results
  const auto &results = buffer.GetOutputTuples();
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(21)));
}
}
}