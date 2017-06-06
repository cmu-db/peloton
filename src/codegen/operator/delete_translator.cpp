//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.cpp
//
// Identification: src/codegen/operator/delete_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/delete_translator.h"

#include "codegen/catalog_proxy.h"
#include "codegen/transaction_runtime_proxy.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace codegen {

DeleteTranslator::DeleteTranslator(const planner::DeletePlan &delete_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      table_ptr_(nullptr),
      delete_plan_(delete_plan),
      table_(*delete_plan_.GetTable()) {
  // Also create the translator for our child.
  context.Prepare(*delete_plan.GetChild(0), pipeline);
}

void DeleteTranslator::Produce() const {
  auto &compilation_context = GetCompilationContext();

  // the child of delete executor will be a scan. call produce function
  // of the child to produce the scanning result
  compilation_context.Produce(*delete_plan_.GetChild(0));
}

/**
 * @brief Generate code that deals with each tuple.
 *
 * @param context The parent consumers.
 * @param row A reference of the code-gen'ed "current row" value.
 */
void DeleteTranslator::Consume(ConsumerContext &context,
                               RowBatch::Row &row) const {
  auto &compilation_context = this->GetCompilationContext();
  auto &codegen = context.GetCodeGen();
  llvm::Value *txn = compilation_context.GetTransactionPtr();

  if (table_ptr_ == nullptr) {
    storage::DataTable *table = delete_plan_.GetTable();
    table_ptr_ = codegen.CallFunc(
        CatalogProxy::_GetTableWithOid::GetFunction(codegen),
        {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
         codegen.Const32(table->GetOid())});
  }

  auto tuple_id = row.GetTID(codegen);
  auto tile_group_id = row.GetBatch().GetTileGroupID();
  auto tile_group = table_.GetTileGroup(codegen, table_ptr_, tile_group_id);

  // Delete a tuple with tuple_id
  codegen.CallFunc(
      TransactionRuntimeProxy::_PerformDelete::GetFunction(codegen),
      {tuple_id, txn, table_ptr_, tile_group});

  // Increase number of tuples by one
  codegen.CallFunc(
      TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunction(codegen),
      {compilation_context.GetExecutorContextPtr()});
}

}  // namespace codegen
}  // namespace peloton
