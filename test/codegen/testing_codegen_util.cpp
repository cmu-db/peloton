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

#include "catalog/table_catalog.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/plan_executor.h"
#include "executor/executor_context.h"
#include "expression/comparison_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "storage/table_factory.h"
#include "codegen/query_cache.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// PELOTON CODEGEN TEST
//===----------------------------------------------------------------------===//

PelotonCodeGenTest::PelotonCodeGenTest(oid_t tuples_per_tilegroup) {
  auto *catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // create test db
  catalog->CreateDatabase(test_db_name, txn);
  test_db = catalog->GetDatabaseWithName(test_db_name, txn);
  // Create test table
  CreateTestTables(txn, tuples_per_tilegroup);

  txn_manager.CommitTransaction(txn);
}

PelotonCodeGenTest::~PelotonCodeGenTest() {
  auto *catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto result = catalog->DropDatabaseWithName(test_db_name, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(ResultType::SUCCESS, result);
  codegen::QueryCache::Instance().Clear();
}

catalog::Column PelotonCodeGenTest::GetTestColumn(uint32_t col_id) const {
  PL_ASSERT(col_id < 4);
  static const uint64_t int_size =
      type::Type::GetTypeSize(type::TypeId::INTEGER);
  static const uint64_t dec_size =
      type::Type::GetTypeSize(type::TypeId::DECIMAL);

  bool is_inlined = true;
  if (col_id == 0) {
    return catalog::Column{type::TypeId::INTEGER, int_size, "COL_A",
                           is_inlined};
  } else if (col_id == 1) {
    return catalog::Column{type::TypeId::INTEGER, int_size, "COL_B",
                           is_inlined};
  } else if (col_id == 2) {
    return catalog::Column{type::TypeId::DECIMAL, dec_size, "COL_C",
                           is_inlined};
  } else {
    return catalog::Column{type::TypeId::VARCHAR, 25, "COL_D", !is_inlined};
  }
}

// Create the test schema for all the tables
std::unique_ptr<catalog::Schema> PelotonCodeGenTest::CreateTestSchema(
    bool add_primary) const {
  // Create the columns
  std::vector<catalog::Column> cols = {GetTestColumn(0), GetTestColumn(1),
                                       GetTestColumn(2), GetTestColumn(3)};

  // Add NOT NULL constraints on COL_A, COL_C, COL_D
  cols[0].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});
  if (add_primary) {
    cols[0].AddConstraint(
        catalog::Constraint{ConstraintType::PRIMARY, "con_primary"});
  }
  cols[2].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});
  cols[3].AddConstraint(
      catalog::Constraint{ConstraintType::NOTNULL, "not_null"});

  // Return the schema
  return std::unique_ptr<catalog::Schema>{new catalog::Schema(cols)};
}

// Create all the test tables, but don't load any data
void PelotonCodeGenTest::CreateTestTables(concurrency::TransactionContext *txn,
                                          oid_t tuples_per_tilegroup) {
  auto *catalog = catalog::Catalog::GetInstance();
  for (int i = 0; i < 4; i++) {
    auto table_schema = CreateTestSchema();
    catalog->CreateTable(test_db_name, test_table_names[i],
                         std::move(table_schema), txn, false,
                         tuples_per_tilegroup);
    test_table_oids.push_back(catalog->GetTableObject(test_db_name,
                                                      test_table_names[i],
                                                      txn)->GetTableOid());
  }
  for (int i = 4; i < 5; i++) {
    auto table_schema = CreateTestSchema(true);
    catalog->CreateTable(test_db_name, test_table_names[i],
                         std::move(table_schema), txn, false,
                         tuples_per_tilegroup);
    test_table_oids.push_back(catalog->GetTableObject(test_db_name,
                                                      test_table_names[i],
                                                      txn)->GetTableOid());
  }
}

void PelotonCodeGenTest::LoadTestTable(oid_t table_id, uint32_t num_rows,
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

void PelotonCodeGenTest::ExecuteSync(
    codegen::Query &query,
    std::unique_ptr<executor::ExecutorContext> executor_context,
    codegen::QueryResultConsumer &consumer) {
  std::mutex mu;
  std::condition_variable cond;
  bool finished = false;
  query.Execute(std::move(executor_context), consumer,
                [&](executor::ExecutionResult) {
                  std::unique_lock<decltype(mu)> lock(mu);
                  finished = true;
                  cond.notify_one();
                });

  std::unique_lock<decltype(mu)> lock(mu);
  cond.wait(lock, [&] { return finished; });
}

codegen::QueryCompiler::CompileStats PelotonCodeGenTest::CompileAndExecute(
    planner::AbstractPlan &plan, codegen::QueryResultConsumer &consumer) {
  codegen::QueryParameters parameters(plan, {});

  // Start a transaction.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Compile the query.
  codegen::QueryCompiler::CompileStats stats;
  auto compiled_query = codegen::QueryCompiler().Compile(
      plan, parameters.GetQueryParametersMap(), consumer, &stats);

  // Execute the query.
  ExecuteSync(*compiled_query,
              std::unique_ptr<executor::ExecutorContext>(
                  new executor::ExecutorContext(txn, std::move(parameters))),
              consumer);

  // Commit the transaction.
  txn_manager.CommitTransaction(txn);

  return stats;
}

codegen::QueryCompiler::CompileStats PelotonCodeGenTest::CompileAndExecuteCache(
    std::shared_ptr<planner::AbstractPlan> plan,
    codegen::QueryResultConsumer &consumer, bool &cached,
    std::vector<type::Value> params) {
  // Start a transaction
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> executor_context(
      new executor::ExecutorContext(txn,
                                    codegen::QueryParameters(*plan, params)));

  // Compile
  codegen::QueryCompiler::CompileStats stats;
  codegen::Query *query = codegen::QueryCache::Instance().Find(plan);
  cached = (query != nullptr);
  if (query == nullptr) {
    codegen::QueryCompiler compiler;
    auto compiled_query = compiler.Compile(
        *plan, executor_context->GetParams().GetQueryParametersMap(), consumer);
    query = compiled_query.get();
    codegen::QueryCache::Instance().Add(plan, std::move(compiled_query));
  }

  // Execute the query.
  ExecuteSync(*query, std::move(executor_context), consumer);

  // Commit the transaction.
  txn_manager.CommitTransaction(txn);

  return stats;
}

ExpressionPtr PelotonCodeGenTest::ConstIntExpr(int64_t val) {
  auto *expr = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(val));
  return ExpressionPtr{expr};
}

ExpressionPtr PelotonCodeGenTest::ConstDecimalExpr(double val) {
  auto *expr = new expression::ConstantValueExpression(
      type::ValueFactory::GetDecimalValue(val));
  return ExpressionPtr{expr};
}

ExpressionPtr PelotonCodeGenTest::ColRefExpr(type::TypeId type,
                                             uint32_t col_id) {
  auto *expr = new expression::TupleValueExpression(type, 0, col_id);
  return ExpressionPtr{expr};
}

ExpressionPtr PelotonCodeGenTest::ColRefExpr(type::TypeId type, bool left,
                                             uint32_t col_id) {
  return ExpressionPtr{
      new expression::TupleValueExpression(type, !left, col_id)};
}

ExpressionPtr PelotonCodeGenTest::CmpExpr(ExpressionType cmp_type,
                                          ExpressionPtr &&left,
                                          ExpressionPtr &&right) {
  auto *expr = new expression::ComparisonExpression(cmp_type, left.release(),
                                                    right.release());
  return ExpressionPtr{expr};
}

ExpressionPtr PelotonCodeGenTest::CmpLtExpr(ExpressionPtr &&left,
                                            ExpressionPtr &&right) {
  return CmpExpr(ExpressionType::COMPARE_LESSTHAN, std::move(left),
                 std::move(right));
}

ExpressionPtr PelotonCodeGenTest::CmpLteExpr(ExpressionPtr &&left,
                                             ExpressionPtr &&right) {
  return CmpExpr(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(left),
                 std::move(right));
}

ExpressionPtr PelotonCodeGenTest::CmpGtExpr(ExpressionPtr &&left,
                                            ExpressionPtr &&right) {
  return CmpExpr(ExpressionType::COMPARE_GREATERTHAN, std::move(left),
                 std::move(right));
}

ExpressionPtr PelotonCodeGenTest::CmpGteExpr(ExpressionPtr &&left,
                                             ExpressionPtr &&right) {
  return CmpExpr(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left),
                 std::move(right));
}

ExpressionPtr PelotonCodeGenTest::CmpEqExpr(ExpressionPtr &&left,
                                            ExpressionPtr &&right) {
  return CmpExpr(ExpressionType::COMPARE_EQUAL, std::move(left),
                 std::move(right));
}

ExpressionPtr PelotonCodeGenTest::OpExpr(ExpressionType op_type,
                                         type::TypeId type,
                                         ExpressionPtr &&left,
                                         ExpressionPtr &&right) {
  switch (op_type) {
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_MOD: {
      return ExpressionPtr{new expression::OperatorExpression(
          op_type, type, left.release(), right.release())};
      break;
    }
    default: {
      throw Exception{"OpExpr only supports (+, -, *, /, %) operations"};
    }
  }
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

}  // namespace test
}  // namespace peloton
