//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.cpp
//
// Identification: src/codegen/delete_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/delete_translator.h"

#include "codegen/catalog_proxy.h"
#include "codegen/deleter.h"
#include "codegen/deleter_proxy.h"
#include "codegen/transaction_runtime_proxy.h"
#include "planner/delete_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

namespace peloton {
namespace codegen {

DeleteTranslator::DeleteTranslator(const planner::DeletePlan &delete_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      delete_plan_(delete_plan),
      table_(*delete_plan_.GetTable()) {
  // Also create the translator for our child.
  context.Prepare(*delete_plan.GetChild(0), pipeline);

  // Register the deleter
  deleter_state_id_ = context.GetRuntimeState().RegisterState(
      "deleter", DeleterProxy::GetType(GetCodeGen()));
}

void DeleteTranslator::InitializeState() {
  auto &codegen = GetCodeGen();

  // The transaction pointer
  llvm::Value *txn_ptr = GetCompilationContext().GetTransactionPtr();

  // Get the table pointer
  storage::DataTable *table = delete_plan_.GetTable();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  // Call Deleter.Init(txn, table)
  llvm::Value *deleter = LoadStatePtr(deleter_state_id_);
  std::vector<llvm::Value *> args = {deleter, txn_ptr, table_ptr};
  codegen.CallFunc(DeleterProxy::_Init::GetFunction(codegen), args);
}

void DeleteTranslator::Produce() const {
  // Call Produce() on our child (a scan), to produce the tuples we'll delete
  GetCompilationContext().Produce(*delete_plan_.GetChild(0));
}

void DeleteTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Call Deleter::Delete(tile_group_id, tuple_offset)
  auto *deleter = LoadStatePtr(deleter_state_id_);
  auto args = {deleter, row.GetTileGroupID(), row.GetTID(codegen)};
  auto *delete_func = DeleterProxy::_Delete::GetFunction(codegen);
  codegen.CallFunc(delete_func, args);

  // Increase processing count
  auto *processed_func =
      TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunction(codegen);
  codegen.CallFunc(processed_func,
                   {GetCompilationContext().GetExecutorContextPtr()});
}

}  // namespace codegen
}  // namespace peloton
