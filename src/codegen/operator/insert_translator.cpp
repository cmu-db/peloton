//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_translator.cpp
//
// Identification: src/codegen/operator/insert_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tuple.h"
#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/inserter_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/tuple_proxy.h"
#include "codegen/operator/insert_translator.h"
#include "codegen/type/sql_type.h"
#include "planner/abstract_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

InsertTranslator::InsertTranslator(const planner::InsertPlan &insert_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), insert_plan_(insert_plan),
      tuple_(*insert_plan.GetTable()) {

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
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});
 
  llvm::Value *executor_ptr = GetCompilationContext().GetExecutorContextPtr();

  // Initialize the inserter with txn and table
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  codegen.Call(InserterProxy::Init, {inserter, txn_ptr, table_ptr,
                                     executor_ptr});
}

void InsertTranslator::Produce() const {
  if (insert_plan_.GetChildrenSize() != 0) {
    // Produce on its child(a scan), to produce the tuples to be inserted
    GetCompilationContext().Produce(*insert_plan_.GetChild(0));
  }
  else {
    auto &codegen = GetCodeGen();
    auto *inserter = LoadStatePtr(inserter_state_id_);

    auto num_tuples = insert_plan_.GetBulkInsertCount();
    for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {
      // Convert the tuple address to the LLVM pointer value
      auto *tuple = insert_plan_.GetTuple(i);
      llvm::Value *tuple_ptr =
          codegen->CreateIntToPtr(codegen.Const64((int64_t)tuple),
                                  TupleProxy::GetType(codegen)->getPointerTo());
 
      // Perform insertion of each tuple through inserter
      codegen.Call(InserterProxy::Insert, {inserter, tuple_ptr});
    }
  }
}

void InsertTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  auto *inserter = LoadStatePtr(inserter_state_id_);

  auto scan =
      static_cast<const planner::AbstractScan *>(insert_plan_.GetChild(0));
  std::vector<const planner::AttributeInfo *> ais;
  scan->GetAttributes(ais);

  auto *tuple_storage = codegen.Call(InserterProxy::ReserveTupleStorage,
                                     {inserter});
  auto *pool = codegen.Call(InserterProxy::GetPool, {inserter});

  // Generate/Materialize tuple data from row and attribute information
  tuple_.Generate(codegen, row, ais, tuple_storage, pool);

  // Call Inserter to insert the reserved tuple storage area
  codegen.Call(InserterProxy::InsertReserved, {inserter});
}

}  // namespace codegen
}  // namespace peloton
