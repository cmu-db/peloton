//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator_test.cpp
//
// Identification: test/codegen/update/update_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/update/update_translator.h"

#include "codegen/transaction_runtime_proxy.h"
#include "codegen/values_runtime_proxy.h"
#include "codegen/value_proxy.h"
#include "codegen/catalog_proxy.h"
#include "codegen/primitive_value_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), update_plan_(update_plan),
      table_(*update_plan.GetTable()) {

  // Also create the translator for our child.
  context.Prepare(*update_plan.GetChild(0), pipeline);

  const auto *project_info = update_plan_.GetProjectInfo();
  // Get target table for update
  target_table_ = update_plan.GetTable();
  // Update primary key or not
  update_primary_key_ = update_plan.GetUpdatePrimaryKey();
  PL_ASSERT(project_info);
  PL_ASSERT(target_table_);
  // Get target & direct list for update
  for (uint32_t i = 0; i < project_info->GetTargetList().size(); i ++) {
    target_list_.emplace_back(project_info->GetTargetList()[i]);
  }
  direct_list_ = project_info->GetDirectMapList();

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  target_val_vec_id_ = runtime_state.RegisterState(
        "updateTargetVec",
        codegen.VectorType(ValueProxy::GetType(codegen),
                           target_list_.size()),
        true);
  col_id_vec_id_ = runtime_state.RegisterState(
        "updateColVec",
        codegen.VectorType(codegen.Int64Type(),
                           target_list_.size()),
        true);

  // Prepare translator for target list
  for (const auto &target : target_list_) {
    const auto &derived_attribute = target.second;
    PL_ASSERT(derived_attribute.expr != nullptr);
    context.Prepare(*derived_attribute.expr);
  }

  // Store runtime params for update operation
  context.StoreDirectList(direct_list_);
  context.StoreTargetList(target_list_);
}

void UpdateTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();
  compilation_context.Produce(*this->update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &,
                               RowBatch::Row &row) const {

  CompilationContext &context = GetCompilationContext();
  CodeGen &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  llvm::Value *catalog_ptr = GetCatalogPtr();

  /*
   * Prepare parameters for calling TransactionRuntime::PerformUpdate
   */

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  llvm::Value *table_ptr =
        codegen.CallFunc(CatalogProxy::_GetTableWithOid::GetFunction(codegen),
                         {catalog_ptr, codegen.Const32(target_table_->GetDatabaseOid()),
                          codegen.Const32(target_table_->GetOid())});

  llvm::Value *tile_group = table_.GetTileGroup(codegen, table_ptr, row.GetTileGroupID());


  llvm::Value *update_primary_key = codegen.ConstBool(update_primary_key_);

  // vector for collecting results from executing target list
  llvm::Value *target_vec = runtime_state.LoadStateValue(codegen, target_val_vec_id_);
  // vector for collecting col ids that are targeted to update
  Vector col_vec{LoadStateValue(col_id_vec_id_),
                 (uint32_t)target_list_.size(), codegen.Int64Type()};


  llvm::Value *target_list_ptr = context.GetTargetListPtr();
  /* Loop for collecting target col ids and corresponding derived values */
  uint32_t idx = 0;
  llvm::Value *target_list_idx = codegen.Const64(idx);
  llvm::Value *target_list_size = codegen.Const64(target_list_.size());

  Loop loop{codegen,
          codegen->CreateICmpULT(target_list_idx, target_list_size),
          {{"targetListIdx", target_list_idx}}};
  {
    target_list_idx = loop.GetLoopVar(0);

    // collect col id
    col_vec.SetValue(codegen, target_list_idx,
                     codegen.Const64(target_list_[idx].first));

    // Derive value using target list (execute target list expr)
    const auto &derived_attribute = target_list_[idx].second;
    codegen::Value val = row.DeriveValue(codegen, *derived_attribute.expr);
    // collect derived value
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARBINARY: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputVarbinary::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue(),
               val.GetLength()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
              ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
              {target_vec, target_list_idx, val.GetValue(),
               val.GetLength()});
        break;
      }
      default: {
        std::string msg =
              StringUtil::Format("Can't serialize value type '%s' at position %u",
                                 TypeIdToString(val.GetType()).c_str(), idx);
        throw Exception{msg};
      }
    }

    target_list_idx = codegen.Const64(++idx);
    loop.LoopEnd(codegen->CreateICmpULT(target_list_idx, target_list_size),
                 {target_list_idx});
  }

  llvm::Value *direct_list_ptr = context.GetDirectListPtr();
  llvm::Value *direct_list_size = codegen.Const64(direct_list_.size());

  llvm::Value *exec_context = context.GetExecContextPtr();

  /*
   * Call TrasactionRuntimeProxy::PerformUpdate
   */

  codegen.CallFunc(
    TransactionRuntimeProxy::_PerformUpdate::GetFunction(codegen),
    {
      txn_ptr, table_ptr, tile_group, row.GetTID(codegen),
      col_vec.GetVectorPtr(), target_vec, update_primary_key,
      target_list_ptr, target_list_size,
      direct_list_ptr, direct_list_size,
      exec_context
    }
  );
}

}  // namespace codegen
}  // namespace peloton
