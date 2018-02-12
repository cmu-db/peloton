//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.cpp
//
// Identification: src/codegen/operator/delete_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/delete_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/deleter_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "planner/delete_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

DeleteTranslator::DeleteTranslator(const planner::DeletePlan &delete_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(delete_plan, context, pipeline),
      table_(*delete_plan.GetTable()) {
  // Also create the translator for our child.
  context.Prepare(*delete_plan.GetChild(0), pipeline);

  // Register the deleter
  deleter_state_id_ = context.GetRuntimeState().RegisterState(
      "deleter", DeleterProxy::GetType(GetCodeGen()));
}

void DeleteTranslator::InitializeQueryState() {
  CodeGen &codegen = GetCodeGen();

  const planner::DeletePlan &plan = GetPlanAs<planner::DeletePlan>();

  // Get the table pointer
  storage::DataTable *table = plan.GetTable();
  llvm::Value *table_ptr = codegen.Call(
      StorageManagerProxy::GetTableWithOid,
      {GetStorageManagerPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  // Call Deleter.Init(txn, table)
  llvm::Value *deleter = LoadStatePtr(deleter_state_id_);
  codegen.Call(DeleterProxy::Init,
               {deleter, table_ptr, GetExecutorContextPtr()});
}

void DeleteTranslator::Produce() const {
  // Call Produce() on our child (a scan), to produce the tuples we'll delete
  GetCompilationContext().Produce(*GetPlan().GetChild(0));
}

void DeleteTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

  // Call Deleter::Delete(tile_group_id, tuple_offset)
  auto *deleter = LoadStatePtr(deleter_state_id_);
  codegen.Call(DeleterProxy::Delete,
               {deleter, row.GetTileGroupID(), row.GetTID(codegen)});
}

}  // namespace codegen
}  // namespace peloton
