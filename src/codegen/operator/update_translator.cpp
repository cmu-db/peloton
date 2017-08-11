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

#include "common/container_tuple.h"
#include "codegen/proxy/catalog_proxy.h"
#include "codegen/proxy/direct_map_proxy.h"
#include "codegen/proxy/target_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/updater_proxy.h"
#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/operator/update_translator.h"
#include "codegen/type/sql_type.h"
#include "planner/project_info.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), update_plan_(update_plan) {
  // Create the translator for our child and derived attributes
  context.Prepare(*update_plan.GetChild(0), pipeline);

  const auto *project_info = update_plan_.GetProjectInfo();
  auto target_list = project_info->GetTargetList();
  for (const auto &target : target_list) {
    const auto &derived_attribute = target.second;
    context.Prepare(*derived_attribute.expr);
  }

  // Prepare for registering RuntimeState
  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  auto column_num = target_list.size();

  target_val_vec_id_ = runtime_state.RegisterState("updateTargetValVec",
      codegen.ArrayType(codegen.Int8Type(), column_num), true);
  column_id_vec_id_ = runtime_state.RegisterState("updateColumnIdVec",
      codegen.ArrayType(codegen.Int32Type(), column_num), true);

  updater_state_id_ = runtime_state.RegisterState("updater",
      UpdaterProxy::GetType(codegen));
}

void UpdateTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  storage::DataTable *table = update_plan_.GetTable();
  llvm::Value *table_ptr = codegen.Call(StorageManagerProxy::GetTableWithOid,
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  const auto *project_info = update_plan_.GetProjectInfo();
  auto *target_vector = project_info->GetTargetList().data();
  auto *target_vector_int = codegen.Const64((int64_t)target_vector);
  llvm::Value *target_vector_ptr = codegen->CreateIntToPtr(
      target_vector_int, TargetProxy::GetType(codegen)->getPointerTo());

  auto target_vector_size = project_info->GetTargetList().size();
  auto *target_vector_size_ptr = codegen.Const32((int32_t)target_vector_size);

  auto *direct_map_vector = project_info->GetDirectMapList().data();
  auto *direct_map_vector_int = codegen.Const64((int64_t)direct_map_vector);
  llvm::Value *direct_map_vector_ptr = codegen->CreateIntToPtr(
      direct_map_vector_int, DirectMapProxy::GetType(codegen)->getPointerTo());

  auto direct_map_vector_size = project_info->GetDirectMapList().size();
  llvm::Value *direct_map_vector_size_ptr =
      codegen.Const32((int32_t)direct_map_vector_size);

  // Initialize the inserter with txn and table
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  codegen.Call(UpdaterProxy::Init, {updater, txn_ptr, table_ptr,
                                    target_vector_ptr, target_vector_size_ptr,
                                    direct_map_vector_ptr,
                                    direct_map_vector_size_ptr});
}

void UpdateTranslator::Produce() const {
  GetCompilationContext().Produce(*update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  // Vector for collecting col ids that are targeted to update
  const auto *project_info = update_plan_.GetProjectInfo();
  auto target_list = project_info->GetTargetList();
  uint32_t column_num = (uint32_t)target_list.size();
  Vector column_ids{LoadStateValue(column_id_vec_id_), column_num,
                    codegen.Int32Type()};

  // Vector for collecting results from executing target list
  llvm::Value *target_vals = LoadStateValue(target_val_vec_id_);

  /* Loop for collecting target col ids and corresponding derived values */
  for (uint32_t i = 0; i < column_num; i++) {
    auto target_id = codegen.Const32(i);

    column_ids.SetValue(codegen, target_id,
                        codegen.Const32(target_list[i].first));
    const auto &derived_attribute = target_list[i].second;
    codegen::Value val = row.DeriveValue(codegen, *derived_attribute.expr);

    const auto &sql_type = val.GetType().GetSqlType();
    auto *set_value_func = sql_type.GetOutputFunction(codegen, val.GetType());
    std::vector<llvm::Value *> args = {target_vals, target_id, val.GetValue()};
    if (val.GetLength() != nullptr) args.push_back(val.GetLength());
    codegen.CallFunc(set_value_func, args);
  }

  auto column_ids_ptr = column_ids.GetVectorPtr();
  auto *executor_context = context.GetExecutorContextPtr();

  // Finally, update!
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  std::vector<llvm::Value *> args = {updater, row.GetTileGroupID(),
                                     row.GetTID(codegen), column_ids_ptr,
                                     target_vals, executor_context};
  if (update_plan_.GetUpdatePrimaryKey() == true) {
    codegen.Call(UpdaterProxy::UpdatePrimaryKey, args);
  }
  else {
    codegen.Call(UpdaterProxy::Update, args);
  }
}

void UpdateTranslator::TearDownState() {
  auto &codegen = GetCodeGen();
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  codegen.Call(UpdaterProxy::TearDown, {updater});
}

}  // namespace codegen
}  // namespace peloton
