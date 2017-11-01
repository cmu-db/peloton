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
#include "codegen/operator/operator_translator.h"
#include "codegen/pipeline.h"
#include "codegen/runtime_state.h"
#include "codegen/table.h"
#include "codegen/table_storage.h"
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
  void TearDownState() override;

  // Get the stringified name of this translator
  std::string GetName() const override { return "Update"; }

 private:
  void SetTargetValue(llvm::Value *target_val_vec, llvm::Value *target_id,
                      type::Type type, llvm::Value *value,
                      llvm::Value *length) const;

 private:
  // Plan
  const planner::UpdatePlan &update_plan_;

  // Target table
  storage::DataTable *target_table_;

  // Boolean value representing whether it is a primary key update or not
  bool update_primary_key_;

  // Runtime state id for the updater
  RuntimeState::StateID updater_state_id_;

  // Tuple storage area
  codegen::TableStorage table_storage_;
};

}  // namespace codegen
}  // namespace peloton
