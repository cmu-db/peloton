//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator_test.cpp
//
// Identification: test/codegen/table_scan_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class TableScanTranslatorTest : public PelotonCodeGenTest {
  std::string all_cols_table_name = "crazy_table";

 public:
  TableScanTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);

    CreateAndLoadAllColsTable();
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  oid_t TestTableId() { return test_table_oids[0]; }

  void CreateAndLoadAllColsTable() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto *catalog = catalog::Catalog::GetInstance();

    // Columns and schema
    const bool is_inlined = true;
    std::vector<catalog::Column> cols = {
        {type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
         "COL_A", is_inlined},
        {type::TypeId::TINYINT, type::Type::GetTypeSize(type::TypeId::TINYINT),
         "COL_B", is_inlined},
        {type::TypeId::SMALLINT,
         type::Type::GetTypeSize(type::TypeId::SMALLINT), "COL_C", is_inlined},
        {type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
         "COL_D", is_inlined},
        {type::TypeId::BIGINT, type::Type::GetTypeSize(type::TypeId::BIGINT),
         "COL_E", is_inlined},
        {type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
         "COL_F", is_inlined},
        {type::TypeId::TIMESTAMP,
         type::Type::GetTypeSize(type::TypeId::TIMESTAMP), "COL_G", is_inlined},
        {type::TypeId::DATE, type::Type::GetTypeSize(type::TypeId::DATE),
         "COL_H", is_inlined},
        {type::TypeId::VARCHAR, 25, "COL_I", !is_inlined}};
    std::unique_ptr<catalog::Schema> schema{new catalog::Schema(cols)};

    // Insert table in catalog
    catalog->CreateTable(test_db_name, all_cols_table_name, std::move(schema),
                         txn);

    auto &table = GetAllColsTable();
    auto *table_schema = table.GetSchema();

    // Insert one row where all columns are NULL
    storage::Tuple tuple{table_schema, true};
    for (uint32_t i = 0; i < table_schema->GetColumnCount(); i++) {
      auto col = table_schema->GetColumn(i);
      tuple.SetValue(i, type::ValueFactory::GetNullValueByType(col.GetType()));
    }

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
        table.InsertTuple(&tuple, txn, &index_entry_ptr);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

    txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    txn_manager.CommitTransaction(txn);
  }

  storage::DataTable &GetAllColsTable() const {
    return *GetDatabase().GetTableWithName(all_cols_table_name);
  }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(TableScanTranslatorTest, AllColumnsScan) {
  //
  // SELECT a, b, c FROM table;
  //

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(TableScanTranslatorTest, AllColumnsScanWithNulls) {
  //
  // SELECT * FROM crazy_table;
  //

  auto &tbl = GetAllColsTable();
  std::vector<oid_t> all_col_ids(tbl.GetSchema()->GetColumnCount());
  std::iota(all_col_ids.begin(), all_col_ids.end(), 0);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&tbl, nullptr, all_col_ids};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{all_col_ids, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());

  // Make sure all column values are NULL
  auto &tuple = buffer.GetOutputTuples()[0];
  for (uint32_t i = 0; i < all_col_ids.size(); i++) {
    auto col_val = tuple.GetValue(i);
    EXPECT_TRUE(col_val.IsNull()) << "Result value: " << col_val.ToString()
                                  << ", expected NULL";
  }
}

TEST_F(TableScanTranslatorTest, SimplePredicate) {
  //
  // SELECT a, b, c FROM table where a >= 20;
  //

  // Setup the predicate
  ExpressionPtr a_gt_20 =
      CmpGteExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));

  // Setup the scan plan node
  auto &table = GetTestTable(TestTableId());
  planner::SeqScanPlan scan{&table, a_gt_20.release(), {0, 1, 2}};

  // Do binding
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

TEST_F(TableScanTranslatorTest, SimplePredicateWithNull) {
  // Insert 10 null rows
  const bool insert_nulls = true;
  LoadTestTable(TestTableId(), 10, insert_nulls);

  //
  // SELECT a, b FROM table where b < 20;
  //

  // Setup the predicate
  ExpressionPtr b_lt_20 =
      CmpLtExpr(ColRefExpr(type::TypeId::INTEGER, 1), ConstIntExpr(20));

  // Setup the scan plan node
  auto &table = GetTestTable(TestTableId());
  planner::SeqScanPlan scan{&table, b_lt_20.release(), {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(2, results.size());

  // First tuple should be (0, 1)
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));

  // Second tuple should be (10, 11)
  EXPECT_EQ(CmpBool::TRUE, results[1].GetValue(0).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(10)));
  EXPECT_EQ(CmpBool::TRUE, results[1].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(11)));
}

TEST_F(TableScanTranslatorTest, PredicateOnNonOutputColumn) {
  //
  // SELECT b FROM table where a >= 40;
  //

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

TEST_F(TableScanTranslatorTest, ScanWithConjunctionPredicate) {
  //
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  //

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

TEST_F(TableScanTranslatorTest, ScanWithAddPredicate) {
  //
  // SELECT a, b FROM table where b = a + 1;
  //

  // Construct the components of the predicate

  // a + 1
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_1_exp = ConstIntExpr(1).release();
  auto *a_plus_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, a_col_exp,
      const_1_exp);

  // b = a + 1
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_plus_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), b_eq_a_plus_1, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithAddColumnsPredicate) {
  //
  // SELECT a, b FROM table where b = a + b;
  //

  // Construct the components of the predicate

  // a + b
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_rhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *a_plus_b = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, a_col_exp,
      b_rhs_col_exp);

  // b = a + b
  auto *b_lhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_eq_a_plus_b = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_lhs_col_exp, a_plus_b);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), b_eq_a_plus_b, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithSubtractPredicate) {
  //
  // SELECT a, b FROM table where a = b - 1;
  //

  // Construct the components of the predicate

  // b - 1
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *const_1_exp = ConstIntExpr(1).release();
  auto *b_minus_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INTEGER, b_col_exp,
      const_1_exp);

  // a = b - 1
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *a_eq_b_minus_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, b_minus_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), a_eq_b_minus_1, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(NumRowsInTestTable(), results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithSubtractColumnsPredicate) {
  //
  // SELECT a, b FROM table where b = b - a;
  //

  // Construct the components of the predicate

  // b - a
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_rhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_minus_a = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::TypeId::INTEGER, b_rhs_col_exp,
      a_col_exp);

  // b = b - a
  auto *b_lhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *b_eq_b_minus_a = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_lhs_col_exp, b_minus_a);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), b_eq_b_minus_a, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithDividePredicate) {
  //
  //   SELECT a, b, c FROM table where a = a / 2;
  //

  // Construct the components of the predicate

  // a / 1
  auto *a_rhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *const_1_exp = ConstIntExpr(2).release();
  auto *a_div_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_DIVIDE, type::TypeId::INTEGER, a_rhs_col_exp,
      const_1_exp);

  // a = a / 1
  auto *a_lhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *a_eq_a_div_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_lhs_col_exp, a_div_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), a_eq_a_div_1, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results - only one output tuple (with a == 0)
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithMultiplyPredicate) {
  //
  // SELECT a, b, c FROM table where a = a  *b;
  //

  // Construct the components of the predicate

  // a  *b
  auto *a_rhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *a_mul_b = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::TypeId::BIGINT, a_rhs_col_exp,
      b_col_exp);

  // a = a  *b
  auto *a_lhs_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *a_eq_a_mul_b = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_lhs_col_exp, a_mul_b);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), a_eq_a_mul_b, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check output results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(TableScanTranslatorTest, ScanWithModuloPredicate) {
  //
  // SELECT a, b, c FROM table where a = b % 1;
  //

  // Construct the components of the predicate

  // b % 1
  auto *b_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 1);
  auto *const_1_exp = ConstIntExpr(1).release();
  auto *b_mod_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::TypeId::DECIMAL, b_col_exp,
      const_1_exp);

  // a = b % 1
  auto *a_col_exp =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0, 0);
  auto *a_eq_b_mod_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, b_mod_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{
      &GetTestTable(TestTableId()), a_eq_b_mod_1, {0, 1, 2}};

  // Do binding
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
                                     type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(CmpBool::TRUE, results[0].GetValue(1).CompareEquals(
                                     type::ValueFactory::GetIntegerValue(1)));
}

}  // namespace test
}  // namespace peloton