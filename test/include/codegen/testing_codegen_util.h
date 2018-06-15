//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_codegen_util.h
//
// Identification: test/include/codegen/testing_codegen_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.h"
#include "codegen/buffering_consumer.h"
#include "codegen/compilation_context.h"
#include "codegen/execution_consumer.h"
#include "codegen/value.h"
#include "common/container_tuple.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/binding_context.h"
#include "storage/data_table.h"
#include "storage/database.h"

#include "common/harness.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

using ExpressionPtr = std::unique_ptr<expression::AbstractExpression>;
using ConstExpressionPtr =
    std::unique_ptr<const expression::AbstractExpression>;

using PlanPtr = std::unique_ptr<planner::AbstractPlan>;
using ConstPlanPtr = std::unique_ptr<const planner::AbstractPlan>;

/**
 * This is a scoped file handle that automatically deletes/removes the file
 * with the given name when the class goes out of scope and the destructor is
 * called.
 */
struct TempFileHandle {
  std::string name;
  TempFileHandle(std::string _name);
  ~TempFileHandle();
};

/**
 * Common base class for all codegen tests. This class has four test tables
 * whose IDs and names are stored in test_table_oids and test_table_names,
 * respectively. The test tables all have the exact schema: column "a" and "b"
 * are integers, column "c" is a decimal, and column "d" is a varchar. The table
 * with the highest OID also has a primary key on column "a".
 */
class PelotonCodeGenTest : public PelotonTest {
 public:
  std::string test_db_name = "peloton_codegen";
  std::vector<std::string> test_table_names = {"table1", "table2", "table3",
                                               "table4", "table5"};
  std::vector<oid_t> test_table_oids;

  PelotonCodeGenTest(oid_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP,
                     peloton::LayoutType layout_type = LayoutType::ROW);

  virtual ~PelotonCodeGenTest();

  // Get the test database
  storage::Database &GetDatabase() const { return *test_db; }

  // Get the test table with the given ID
  storage::DataTable &GetTestTable(oid_t table_id) const {
    return *GetDatabase().GetTableWithOid(static_cast<uint32_t>(table_id));
  }

  // Get the layout table
  storage::DataTable *GetLayoutTable() const { return layout_table; }

  // Create the schema (common among all tables)
  catalog::Column GetTestColumn(uint32_t col_id) const;

  std::unique_ptr<catalog::Schema> CreateTestSchema(
      bool add_primary = false) const;

  // Create the test tables
  void CreateTestTables(concurrency::TransactionContext *txn,
                        oid_t tuples_per_tilegroup,
                        peloton::LayoutType layout_type);

  // Load the given table with the given number of rows
  void LoadTestTable(oid_t table_id, uint32_t num_rows,
                     bool insert_nulls = false);

  // Load tables with the specified layout
  void CreateAndLoadTableWithLayout(peloton::LayoutType layout_type,
                                    oid_t tuples_per_tilegroup,
                                    oid_t tile_group_count, oid_t column_count,
                                    bool is_inlined);

  // Compile and execute the given plan
  codegen::QueryCompiler::CompileStats CompileAndExecute(
      planner::AbstractPlan &plan, codegen::ExecutionConsumer &consumer);

  codegen::QueryCompiler::CompileStats CompileAndExecuteCache(
      std::shared_ptr<planner::AbstractPlan> plan,
      codegen::ExecutionConsumer &consumer, bool &cached,
      std::vector<type::Value> params = {});

  //===--------------------------------------------------------------------===//
  // Helpers
  //===--------------------------------------------------------------------===//
  ExpressionPtr ConstIntExpr(int64_t val);

  ExpressionPtr ConstDecimalExpr(double val);

  ExpressionPtr ColRefExpr(type::TypeId type, uint32_t col_id);
  ExpressionPtr ColRefExpr(type::TypeId type, bool left, uint32_t col_id);

  ExpressionPtr CmpExpr(ExpressionType cmp_type, ExpressionPtr &&left,
                        ExpressionPtr &&right);

  ExpressionPtr CmpLtExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpLteExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpGtExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpGteExpr(ExpressionPtr &&left, ExpressionPtr &&right);
  ExpressionPtr CmpEqExpr(ExpressionPtr &&left, ExpressionPtr &&right);

  ExpressionPtr OpExpr(ExpressionType op_type, type::TypeId type,
                       ExpressionPtr &&left, ExpressionPtr &&right);

 private:
  storage::Database *test_db;
  storage::DataTable *layout_table;
};

//===----------------------------------------------------------------------===//
// A query consumer that prints the tuples to standard out
//===----------------------------------------------------------------------===//
class Printer : public codegen::ExecutionConsumer {
 public:
  Printer(std::vector<oid_t> cols, planner::BindingContext &context) {
    for (oid_t col_id : cols) {
      ais_.push_back(context.Find(col_id));
    }
  }

  bool SupportsParallelExec() const override { return false; }

  // None of these are used, except ConsumeResult()
  void InitializeQueryState(codegen::CompilationContext &) override {}
  void TearDownQueryState(codegen::CompilationContext &) override {}

  void Prepare(codegen::CompilationContext &ctx) override;
  void ConsumeResult(codegen::ConsumerContext &ctx,
                     codegen::RowBatch::Row &) const override;

 private:
  std::vector<const planner::AttributeInfo *> ais_;
};

}  // namespace test
}  // namespace peloton
