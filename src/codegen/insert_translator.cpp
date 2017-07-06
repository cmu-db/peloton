//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_translator.cpp
//
// Identification: src/codegen/insert_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/catalog_proxy.h"
#include "codegen/insert_translator.h"
#include "codegen/inserter_proxy.h"
#include "codegen/type.h"
#include "codegen/transaction_runtime_proxy.h"
#include "codegen/tuple_runtime_proxy.h"
#include "planner/abstract_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

InsertTranslator::InsertTranslator(const planner::InsertPlan &insert_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), insert_plan_(insert_plan) {

  // Create the translator for its child only when there is a child
  if (insert_plan.GetChildrenSize() != 0) {
    context.Prepare(*insert_plan.GetChild(0), pipeline);
  }

  // Register the inserter's runtime state to approach it throughout this
  inserter_state_id_ = context.GetRuntimeState().RegisterState(
      "inserter", InserterProxy::GetType(GetCodeGen()));
}

void InsertTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  storage::DataTable *table = insert_plan_.GetTable();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});
 
  llvm::Value *executor_ptr = GetCompilationContext().GetExecutorContextPtr();

  // Initialize the inserter with txn and table
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  std::vector<llvm::Value *> args = {inserter, txn_ptr, table_ptr,
                                     executor_ptr};
  codegen.CallFunc(InserterProxy::_Init::GetFunction(codegen), args);
}

void InsertTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto *inserter = LoadStatePtr(inserter_state_id_);

  if (insert_plan_.GetChildrenSize() != 0) {
    // Let the inserter prepare for tuple-at-a-time insertions
    codegen.CallFunc(InserterProxy::_CreateTuple::GetFunction(codegen),
                     {inserter});

    // Produce on its child(a scan), to produce the tuples to be inserted
    GetCompilationContext().Produce(*insert_plan_.GetChild(0));
  }
  else {
    // Let the inserter have all the tuple references to be inserted
    auto num_tuples = insert_plan_.GetBulkInsertCount();
    for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {

      // Convert the tuple address to the LLVM pointer value
      auto *tuple = insert_plan_.GetTuple(i);
      auto *tuple_int = codegen.Const64((int64_t)tuple);
      llvm::Value *tuple_ptr = codegen->CreateIntToPtr(tuple_int,
          TupleProxy::GetType(codegen)->getPointerTo());
 
      // Perform insert tuples set in the inserter
      auto *insert_func = InserterProxy::_Insert::GetFunction(codegen);
      codegen.CallFunc(insert_func, {inserter, tuple_ptr});
    }
  }
}

void InsertTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Materialize row values into the tuple created in the inserter
  auto *inserter = LoadStatePtr(inserter_state_id_);
  auto *tuple_data_func = InserterProxy::_GetTupleData::GetFunction(codegen);
  auto *tuple_data = codegen.CallFunc(tuple_data_func, {inserter});

  auto *pool_func = InserterProxy::_GetPool::GetFunction(codegen);
  auto *pool = codegen.CallFunc(pool_func, {inserter});

  Materialize(row, tuple_data, pool);

  // Insert the materialized tuple, which has been created in the inserter
  auto *insert_func = InserterProxy::_InsertTuple::GetFunction(codegen);
  codegen.CallFunc(insert_func, {inserter});
}

void InsertTranslator::TearDownState() {
  auto &codegen = GetCodeGen();
 
  // Finalize the inserter
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  std::vector<llvm::Value *> args = {inserter};
  codegen.CallFunc(InserterProxy::_Destroy::GetFunction(codegen), args);
}

void InsertTranslator::Materialize(RowBatch::Row &row, llvm::Value *data,
                                   llvm::Value *pool) const {
  auto &codegen = GetCodeGen();
  auto scan = 
      static_cast<const planner::AbstractScan *>(insert_plan_.GetChild(0));
  std::vector<const planner::AttributeInfo *> ais;
  scan->GetAttributes(ais);

  auto *table = insert_plan_.GetTable();
  auto *schema = table->GetSchema();
  auto num_columns = schema->GetColumnCount();
  for (oid_t i = 0; i < num_columns; i++) {
    auto offset = schema->GetOffset(i);
    auto *ai = ais.at(i);
    codegen::Value v = row.DeriveValue(codegen, ai);
    llvm::Type *val_type, *len_type;
    Type::GetTypeForMaterialization(codegen, v.GetType(), val_type, len_type);
    llvm::Value *ptr = codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(),
                                                           data, offset);
    switch (v.GetType()) {
      case type::Type::TypeId::TINYINT:
      case type::Type::TypeId::SMALLINT:
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER:
      case type::Type::TypeId::TIMESTAMP:
      case type::Type::TypeId::BIGINT:
      case type::Type::TypeId::DECIMAL: {
        auto val_ptr = codegen->CreateBitCast(ptr, val_type->getPointerTo());
        codegen->CreateStore(v.GetValue(), val_ptr);
        break;
      }
      case type::Type::TypeId::VARBINARY:
      case type::Type::TypeId::VARCHAR: {
        PL_ASSERT(v.GetLength() != nullptr);
        auto func = TupleRuntimeProxy::_MaterializeVarLen::GetFunction(codegen);
        auto val_ptr = codegen->CreateBitCast(ptr, codegen.CharPtrType());
        codegen.CallFunc(func, {v.GetValue(), v.GetLength(), val_ptr, pool});
        break;
      }
      default: {
        auto msg = StringUtil::Format(
            "Can't materialize type '%s' at column position(%u)",
            TypeIdToString(v.GetType()).c_str(), i);
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
