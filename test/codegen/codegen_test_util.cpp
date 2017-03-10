//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen_test_utils.cpp
//
// Identification: test/codegen/codegen_test_utils.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen_test_util.h"

#include "type/value_factory.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/values_runtime_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace test {

expression::ConstantValueExpression *CodegenTestUtils::ConstIntExpression(
    int64_t val) {
  return new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(val));
}

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
  catalog::Catalog::GetInstance()->DropDatabaseWithOid(test_db_id);
}

// Create all the test tables, but don't load any data
void PelotonCodeGenTest::CreateTestTables() {
  const int tuples_per_tilegroup = 32;
  const bool adapt_table = false;
  const bool own_schema = true;

  auto *table1_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  auto *table1 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), test_table1_id, table1_schema, "table1",
      tuples_per_tilegroup, own_schema, adapt_table);

  auto *table2_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  auto *table2 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), test_table2_id, table2_schema, "table2",
      tuples_per_tilegroup, own_schema, adapt_table);

  auto *table3_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  auto *table3 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), test_table3_id, table3_schema, "table3",
      tuples_per_tilegroup, own_schema, adapt_table);

  auto *table4_schema =
      new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
                           TestingExecutorUtil::GetColumnInfo(1),
                           TestingExecutorUtil::GetColumnInfo(2),
                           TestingExecutorUtil::GetColumnInfo(3)});
  auto *table4 = storage::TableFactory::GetDataTable(
      GetDatabase().GetOid(), test_table4_id, table4_schema, "table4",
      tuples_per_tilegroup, own_schema, adapt_table);

  GetDatabase().AddTable(table1, false);
  GetDatabase().AddTable(table2, false);
  GetDatabase().AddTable(table3, false);
  GetDatabase().AddTable(table4, false);
}

void PelotonCodeGenTest::LoadTestTable(uint32_t table_id, uint32_t num_rows) {
  auto &test_table = GetTestTable(table_id);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  TestingExecutorUtil::PopulateTable(&test_table, num_rows, false, false, false,
                                     txn);
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
  compiled_query->Execute(*txn, consumer_state);

  txn_manager.CommitTransaction(txn);

  return stats;
}

//===----------------------------------------------------------------------===//
// BUFFERING CONSUMER
//===----------------------------------------------------------------------===//

// Buffer the tuple into the output buffer in the state
void BufferingConsumer::BufferTuple(char *state, type::Value *vals,
                                    uint32_t num_vals) {
  BufferingState *buffer_state = reinterpret_cast<BufferingState *>(state);
  buffer_state->output->emplace_back(vals, num_vals);
}

// Proxy to BufferingConsumer::BufferTuple
llvm::Function *BufferingConsumer::_BufferTupleProxy::GetFunction(
    codegen::CodeGen &codegen) {
  const std::string &fn_name =
#ifdef __APPLE__
      "_ZN7peloton4test17BufferingConsumer11BufferTupleEPcPNS_4type5ValueEj";
#else
      "_ZN7peloton4test17BufferingConsumer11BufferTupleEPcPNS_4type5ValueEj";
#endif

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> args = {
      codegen.CharPtrType(),
      codegen::ValueProxy::GetType(codegen)->getPointerTo(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

void BufferingConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();

  // Introduce the consumer state as global
  consumer_state_id_ = runtime_state.RegisterState(
      "consumerState", codegen.CharPtrType(), false);

  // Introduce our output tuple buffer as local (on stack)
  auto *value_type = codegen::ValueProxy::GetType(codegen);
  tuple_output_state_id_ = runtime_state.RegisterState(
      "output", codegen.VectorType(value_type, ais_.size()), true);
}

void BufferingConsumer::ConsumeResult(codegen::ConsumerContext &ctx,
                                      codegen::RowBatch::Row &row) const {
  auto &codegen = ctx.GetCodeGen();
  auto *tuple_buffer_ = GetStateValue(ctx, tuple_output_state_id_);

  for (size_t i = 0; i < ais_.size(); i++) {
    codegen::Value val = row.GetAttribute(codegen, ais_[i]);
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP:
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue(),
             val.GetLength()});
        break;
      }
      default: {
        throw Exception{"Can't serialize type " +
            TypeIdToString(val.GetType()) + " at position " +
            std::to_string(i)};
      }
    }
  }

  // Append the tuple to the output buffer (by calling BufferTuple(...))
  std::vector<llvm::Value *> args = {GetStateValue(ctx, consumer_state_id_),
                                     tuple_buffer_,
                                     codegen.Const32(ais_.size())};
  codegen.CallFunc(_BufferTupleProxy::GetFunction(codegen), args);
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
    codegen::Value val = row.GetAttribute(codegen, ai);
    assert(val.GetType() != type::Type::TypeId::INVALID);
    switch (val.GetType()) {
      case type::Type::TypeId::BOOLEAN:
      case type::Type::TypeId::TINYINT:
      case type::Type::TypeId::SMALLINT:
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        format.append("%d");
        break;
      }
      case type::Type::TypeId::TIMESTAMP:
      case type::Type::TypeId::BIGINT: {
        format.append("%ld");
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        format.append("%lf");
        break;
      }
      case type::Type::TypeId::VARCHAR: {
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