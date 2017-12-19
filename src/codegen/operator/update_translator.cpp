//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.cpp
//
// Identification: src/codegen/operator/update_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/lang/if.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/target_proxy.h"
#include "codegen/proxy/updater_proxy.h"
#include "codegen/operator/update_translator.h"
#include "codegen/table_storage.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), update_plan_(update_plan),
      table_storage_(*update_plan.GetTable()->GetSchema()) {
  // Create the translator for our child and derived attributes
  context.Prepare(*update_plan.GetChild(0), pipeline);

  const auto *project_info = update_plan_.GetProjectInfo();
  auto target_list = project_info->GetTargetList();
  for (const auto &target : target_list) {
    const auto &derived_attribute = target.second;
    context.Prepare(*derived_attribute.expr);
  }
  // Prepare for updater
  updater_state_id_ = context.GetRuntimeState().RegisterState("updater",
      UpdaterProxy::GetType(GetCodeGen()));
}

bool IsTarget(const TargetList &target_list, uint32_t index) {
  for (const auto &target : target_list) {
    if (target.first == index) {
      return true;
    }
  }
  return false;
}

void UpdateTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  // Prepare for all the information to be handed over to the updater
  // Get the transaction pointer
  // Get the table object pointer
  storage::DataTable *table = update_plan_.GetTable();
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
      {GetStorageManagerPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  // Get the target list's raw vectors and their sizes
  // : this is required when installing a new version at updater
  const auto *project_info = update_plan_.GetProjectInfo();
  llvm::Value *target_vector_ptr = codegen->CreateIntToPtr(
      codegen.Const64((int64_t)project_info->GetTargetList().data()),
      TargetProxy::GetType(codegen)->getPointerTo());
  llvm::Value *target_vector_size_ptr =
      codegen.Const32((int32_t)project_info->GetTargetList().size());

  // Initialize the inserter with txn and table
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  codegen.Call(UpdaterProxy::Init, {updater, table_ptr,
                                    context.GetExecutorContextPtr(),
                                    target_vector_ptr, target_vector_size_ptr});
}

void UpdateTranslator::Produce() const {
  GetCompilationContext().Produce(*update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  const auto *project_info = update_plan_.GetProjectInfo();
  auto target_list = project_info->GetTargetList();
  auto direct_map_list = project_info->GetDirectMapList();
  uint32_t column_num =
      static_cast<uint32_t>(target_list.size() + direct_map_list.size());
  auto &ais = update_plan_.GetAttributeInfos();

  // Collect all the column values
  std::vector<codegen::Value> values;
  for (uint32_t i = 0, target_id = 0; i < column_num; i++) {
    codegen::Value val;
    if (IsTarget(target_list, i)) {
      // Set the value for the update
      const auto &derived_attribute = target_list[target_id].second;
      val = row.DeriveValue(codegen, *derived_attribute.expr);
      target_id++;
    } else {
      val = row.DeriveValue(codegen, ais[i]);
    }
    values.push_back(val);
  }

  // Get the tuple pointer from the updater
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  llvm::Value *tuple_ptr;
  std::vector<llvm::Value *> prep_args = {updater, row.GetTileGroupID(),
                                          row.GetTID(codegen)};
  if (update_plan_.GetUpdatePrimaryKey() == false) {
    tuple_ptr = codegen.Call(UpdaterProxy::Prepare, prep_args);
  } else {
    tuple_ptr = codegen.Call(UpdaterProxy::PreparePK, prep_args);
  }

  // Update only when we have tuple_ptr, otherwise it is not allowed to update
  llvm::Value *prepare_cond = codegen->CreateICmpNE(
      codegen->CreatePtrToInt(tuple_ptr, codegen.Int64Type()),
      codegen.Const64(0));
  lang::If prepare_success{codegen, prepare_cond};
  {
    auto *pool_ptr = codegen.Call(UpdaterProxy::GetPool, {updater});

    // Build up a tuple storage
    table_storage_.StoreValues(codegen, tuple_ptr, values, pool_ptr);
  
    // Finally, update with help from the Updater
    std::vector<llvm::Value *> update_args = {updater};
    if (update_plan_.GetUpdatePrimaryKey() == false) {
      codegen.Call(UpdaterProxy::Update, update_args);
    } else {
      codegen.Call(UpdaterProxy::UpdatePK, update_args);
    }
  }
  prepare_success.EndIf();
}

void UpdateTranslator::TearDownState() {
  auto &codegen = GetCodeGen();
  // Tear down the updater
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  codegen.Call(UpdaterProxy::TearDown, {updater});
}

}  // namespace codegen
}  // namespace peloton
