//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_codegen_util.cpp
//
// Identification: test/codegen/testing_codegen_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// PELOTON CODEGEN TEST
//===----------------------------------------------------------------------===//

PelotonCodeGenTest::PelotonCodeGenTest()
    : test_db(new storage::Database(test_db_id)) {
  // Create test table
  CreateTestTables();

  // Add DB to catalog
  catalog::Catalog::GetInstance()->AddDatabase(test_db);
}

PelotonCodeGenTest::~PelotonCodeGenTest() {
  auto *catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto result = catalog->DropDatabaseWithOid(test_db_id, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(ResultType::SUCCESS, result);
}

// Create the test schema for all the tables
std::unique_ptr<catalog::Schema> PelotonCodeGenTest::CreateTestSchema() const {
  bool is_inlined = true;

  // Create the columns
  static const uint32_t int_size = type::Type::GetTypeSize(type::TypeId::INTEGER);
  static const uint32_t dec_size = type::Type::GetTypeSize(type::TypeId::DECIMAL);
  std::vector<catalog::Column> cols = {
      catalog::Column{type::TypeId::INTEGER, int_size, "COL_A", is_inlined},
      catalog::Column{type::TypeId::INTEGER, int_size, "COL_B", is_inlined},
      catalog::Column{type::TypeId::DECIMAL, dec_size, "COL_C", is_inlined},
      catalog::Column{type::TypeId::VARCHAR, 25, "COL_D", !is_inlined}};

  // Add NOT NULL constraints on COL_A, COL_C, COL_D
  cols[0].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});
  cols[2].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});
  cols[3].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});

  // Return the schema
  return std::unique_ptr<catalog::Schema>{new catalog::Schema(cols)};
}

// Create all the test tables, but don't load any data
void PelotonCodeGenTest::CreateTestTables() {
  const int tuples_per_tilegroup = 32;
  const bool adapt_table = false;
  const bool own_schema = true;

  // Table 1
  auto table1_schema = CreateTestSchema();
  auto *table1 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), (uint32_t)TableId::_1, table1_schema.release(),
      "table1", tuples_per_tilegroup, own_schema, adapt_table);

  // Table 2
  auto table2_schema = CreateTestSchema();
  auto *table2 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), (uint32_t)TableId::_2, table2_schema.release(),
      "table2", tuples_per_tilegroup, own_schema, adapt_table);

  // Table 3
  auto table3_schema = CreateTestSchema();
  auto *table3 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), (uint32_t)TableId::_3, table3_schema.release(),
      "table3", tuples_per_tilegroup, own_schema, adapt_table);

  // Table 4
  auto table4_schema = CreateTestSchema();
  auto *table4 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), (uint32_t)TableId::_4, table4_schema.release(),
      "table4", tuples_per_tilegroup, own_schema, adapt_table);

  // Insert all tables into the test database
  GetDatabase().AddTable(table1, false);
  GetDatabase().AddTable(table2, false);
  GetDatabase().AddTable(table3, false);
  GetDatabase().AddTable(table4, false);
}

void PelotonCodeGenTest::LoadTestTable(TableId table_id, uint32_t num_rows,
                                       bool insert_nulls) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  auto &test_table = GetTestTable(table_id);
  auto *table_schema = test_table.GetSchema();
  size_t curr_size = test_table.GetTupleCount();

  auto col_val =
      [](uint32_t tuple_id, uint32_t col_id) { return 10 * tuple_id + col_id; };

  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  for (uint32_t rowid = curr_size; rowid < (curr_size + num_rows); rowid++) {
    // The input tuple
    storage::Tuple tuple{table_schema, allocate};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(col_val(rowid, 0)));
    if (insert_nulls) {
      auto &col = table_schema->GetColumn(1);
      tuple.SetValue(1, type::ValueFactory::GetNullValueByType(col.GetType()));
    } else {
      tuple.SetValue(1, type::ValueFactory::GetIntegerValue(col_val(rowid, 1)));
    }
    tuple.SetValue(2, type::ValueFactory::GetDecimalValue(col_val(rowid, 2)));

    // In case of random, make sure this column has duplicated values
    auto string_value =
        type::ValueFactory::GetVarcharValue(std::to_string(col_val(rowid, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
        test_table.InsertTuple(&tuple, txn, &index_entry_ptr);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
  }

  txn_manager.CommitTransaction(txn);
}

codegen::QueryCompiler::CompileStats PelotonCodeGenTest::CompileAndExecute(
    const planner::AbstractPlan &plan, codegen::QueryResultConsumer &consumer,
    char *consumer_state) {
  // Start a transaction
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Compile
  codegen::QueryCompiler::CompileStats stats;
  codegen::QueryCompiler compiler;
  auto compiled_query = compiler.Compile(plan, consumer, &stats);

  // Run
  compiled_query->Execute(*txn,
                          std::unique_ptr<executor::ExecutorContext> (
                             new executor::ExecutorContext{txn}).get(),
                          consumer_state);

  txn_manager.CommitTransaction(txn);
  return stats;
}

std::unique_ptr<expression::AbstractExpression>
PelotonCodeGenTest::ConstIntExpr(int64_t val) {
  auto *expr = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(val));
  return std::unique_ptr<expression::AbstractExpression>{expr};
}

std::unique_ptr<expression::AbstractExpression>
PelotonCodeGenTest::ConstDecimalExpr(double val) {
  auto *expr = new expression::ConstantValueExpression(
      type::ValueFactory::GetDecimalValue(val));
  return std::unique_ptr<expression::AbstractExpression>{expr};
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::ColRefExpr(
    type::TypeId type, uint32_t col_id) {
  auto *expr = new expression::TupleValueExpression(type, 0, col_id);
  return std::unique_ptr<expression::AbstractExpression>{expr};
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::CmpExpr(
    ExpressionType cmp_type,
    std::unique_ptr<expression::AbstractExpression> &&left,
    std::unique_ptr<expression::AbstractExpression> &&right) {
  auto *expr = new expression::ComparisonExpression(cmp_type, left.release(),
                                                    right.release());
  return std::unique_ptr<expression::AbstractExpression>{expr};
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::CmpLtExpr(
    std::unique_ptr<expression::AbstractExpression> &&left,
    std::unique_ptr<expression::AbstractExpression> &&right) {
  return CmpExpr(ExpressionType::COMPARE_LESSTHAN, std::move(left),
                 std::move(right));
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::CmpGtExpr(
    std::unique_ptr<expression::AbstractExpression> &&left,
    std::unique_ptr<expression::AbstractExpression> &&right) {
  return CmpExpr(ExpressionType::COMPARE_GREATERTHAN, std::move(left),
                 std::move(right));
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::CmpGteExpr(
    std::unique_ptr<expression::AbstractExpression> &&left,
    std::unique_ptr<expression::AbstractExpression> &&right) {
  return CmpExpr(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left),
                 std::move(right));
}

std::unique_ptr<expression::AbstractExpression> PelotonCodeGenTest::CmpEqExpr(
    std::unique_ptr<expression::AbstractExpression> &&left,
    std::unique_ptr<expression::AbstractExpression> &&right) {
  return CmpExpr(ExpressionType::COMPARE_EQUAL, std::move(left),
                 std::move(right));
}

//===----------------------------------------------------------------------===//
// PRINTER
//===----------------------------------------------------------------------===//

void Printer::ConsumeResult(codegen::ConsumerContext &ctx,
                            codegen::RowBatch::Row &row) const {
  codegen::CodeGen &codegen = ctx.GetCodeGen();

  // Iterate over the attributes, constructing the printf format along the way
  std::string format = "[";
  std::vector<llvm::Value *> cols;
  bool first = true;
  for (const auto *ai : ais_) {
    // Handle row format
    if (!first) {
      format.append(", ");
    }
    first = false;
    codegen::Value val = row.DeriveValue(codegen, ai);
    PL_ASSERT(val.GetType().type_id != peloton::type::TypeId::INVALID);
    switch (val.GetType().type_id) {
      case type::TypeId::BOOLEAN:
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::DATE:
      case type::TypeId::INTEGER: {
        format.append("%d");
        break;
      }
      case type::TypeId::TIMESTAMP:
      case type::TypeId::BIGINT: {
        format.append("%ld");
        break;
      }
      case type::TypeId::DECIMAL: {
        format.append("%lf");
        break;
      }
      case type::TypeId::VARCHAR: {
        cols.push_back(val.GetLength());
        format.append("'%.*s'");
        break;
      }
      default:
        throw std::runtime_error("Can't print that shit, bruh");
    }
    cols.push_back(val.GetValue());
  }
  format.append("]\n");

  // Make the printf call
  codegen.CallPrintf(format, cols);
}

//===----------------------------------------------------------------------===//
// COUNTING CONSUMER
//===----------------------------------------------------------------------===//

void CountingConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  counter_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.Int64Type());
}

void CountingConsumer::InitializeState(codegen::CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto *state_ptr = GetCounter(codegen, context.GetRuntimeState());
  codegen->CreateStore(codegen.Const64(0), state_ptr);
}

// Increment the counter
void CountingConsumer::ConsumeResult(codegen::ConsumerContext &context,
                                     codegen::RowBatch::Row &) const {
  auto &codegen = context.GetCodeGen();

  auto *counter_ptr = GetCounter(context);
  auto *new_count =
      codegen->CreateAdd(codegen->CreateLoad(counter_ptr), codegen.Const64(1));
  codegen->CreateStore(new_count, counter_ptr);
}

}  // namespace test
}  // namespace peloton
