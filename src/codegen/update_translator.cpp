//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.cpp
//
// Identification: src/codegen/update_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/catalog_proxy.h"
#include "codegen/direct_map_proxy.h"
#include "codegen/target_proxy.h"
#include "codegen/transaction_runtime_proxy.h"
#include "codegen/update_translator.h"
#include "codegen/updater_proxy.h"
#include "codegen/value_proxy.h"
#include "codegen/values_runtime_proxy.h"
#include "planner/project_info.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      update_plan_(update_plan) {
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

  // Vectors are 
  target_val_vec_id_ = runtime_state.RegisterState( "updateTargetValVec",
      codegen.VectorType(ValueProxy::GetType(codegen), column_num), true);
  column_id_vec_id_ = runtime_state.RegisterState( "updateColumnIdVec",
      codegen.VectorType(codegen.Int64Type(), column_num), true);

  updater_state_id_ = runtime_state.RegisterState("updater",
      UpdaterProxy::GetType(codegen));
}

void UpdateTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  storage::DataTable *table = update_plan_.GetTable();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
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

  llvm::Value *update_primary_key_ptr = codegen.ConstBool(
      update_plan_.GetUpdatePrimaryKey());

  // Initialize the inserter with txn and table
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  auto *init_func = UpdaterProxy::_Init::GetFunction(codegen);
  codegen.CallFunc(init_func, {updater, txn_ptr, table_ptr, target_vector_ptr,
                               target_vector_size_ptr, direct_map_vector_ptr,
                               direct_map_vector_size_ptr,
                               update_primary_key_ptr});
}

void UpdateTranslator::Produce() const {
  GetCompilationContext().Produce(*update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  storage::DataTable *table = update_plan_.GetTable();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});
  Table codegen_table(*table);
  llvm::Value *tile_group_ptr =
      codegen_table.GetTileGroup(codegen, table_ptr, row.GetTileGroupID());

  // vector for collecting col ids that are targeted to update
  const auto *project_info = update_plan_.GetProjectInfo();
  auto target_list = project_info->GetTargetList();
  uint32_t column_num = (uint32_t)target_list.size();
  Vector column_ids{LoadStateValue(column_id_vec_id_), column_num,
                    codegen.Int64Type()};

  // vector for collecting results from executing target list
  llvm::Value *target_vals = LoadStateValue(target_val_vec_id_);

  /* Loop for collecting target col ids and corresponding derived values */
  for (uint32_t i = 0; i < column_num; i++) {
    auto target_id = codegen.Const64(i);

    column_ids.SetValue(codegen, target_id,
                        codegen.Const64(target_list[i].first));

    const auto &derived_attribute = target_list[i].second;
    codegen::Value val = row.DeriveValue(codegen, *derived_attribute.expr);
    SetTargetValue(target_vals, target_id, val.GetType(), val.GetValue(),
                   val.GetLength());
  }

  auto column_ids_ptr = column_ids.GetVectorPtr();
  auto *executor_context = context.GetExecutorContextPtr();

  // Update
  llvm::Value *updater = LoadStatePtr(updater_state_id_);
  auto *update_func = UpdaterProxy::_Update::GetFunction(codegen);
  codegen.CallFunc(update_func, {updater, tile_group_ptr, row.GetTID(codegen),
                                 column_ids_ptr, target_vals,
                                 executor_context});
}

void UpdateTranslator::SetTargetValue(llvm::Value *target_val_vec,
    llvm::Value *target_id, type::Type::TypeId type, llvm::Value *value,
    llvm::Value *length) const {
  auto &codegen = GetCodeGen();
  llvm::Function *func;
  switch (type) {
    case type::Type::TypeId::TINYINT: {
      func = ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::SMALLINT: {
      func = ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::DATE:
    case type::Type::TypeId::INTEGER: {
      func = ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::TIMESTAMP: {
      func = ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::BIGINT: {
      func = ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::DECIMAL: {
      func = ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value});
      break;
    }
    case type::Type::TypeId::VARBINARY: {
      func = ValuesRuntimeProxy::_OutputVarbinary::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value, length});
      break;
    }
    case type::Type::TypeId::VARCHAR: {
      func = ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen);
      codegen.CallFunc(func, {target_val_vec, target_id, value, length});
      break;
    }
    default: {
      std::string msg = StringUtil::Format("Can't serialize value type '%s'",
                             TypeIdToString(type).c_str());
      throw Exception{msg};
    }
  }
}

}  // namespace codegen
}  // namespace peloton
