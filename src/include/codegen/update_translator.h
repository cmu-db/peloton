//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.h
//
// Identification: src/include/codegen/update_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"
#include "codegen/pipeline.h"
#include "codegen/runtime_state.h"
#include "codegen/table.h"
#include "planner/update_plan.h"

namespace peloton {
namespace codegen {

class UpdateTranslator : public OperatorTranslator {
 public:
  // Constructor
  UpdateTranslator(const planner::UpdatePlan &plan, CompilationContext &context,
                   Pipeline &pipeline);

  // State initialization
  void InitializeState() override;

  // No helper functions
  void DefineAuxiliaryFunctions() override {}

  // Produce
  void Produce() const override;

  // Consume : No Cosume() override for Batch
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // No state finalization
  void TearDownState() override {}

  // Get the stringified name of this translator
  std::string GetName() const override { return "Update"; }

 private:
  void SetTargetValue(llvm::Value *target_val_vec, llvm::Value *target_id,
                      type::Type::TypeId type, llvm::Value *value,
                      llvm::Value *length) const;

 private:
  const planner::UpdatePlan &update_plan_;

  storage::DataTable *target_table_;
  bool update_primary_key_;
  TargetList *target_list_;
  DirectMapList *direct_map_list_;

  RuntimeState::StateID target_val_vec_id_;
  RuntimeState::StateID column_id_vec_id_;

  RuntimeState::StateID updater_state_id_;
};

}  // namespace codegen
}  // namespace peloton
