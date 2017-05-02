//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.h
//
// Identification: src/include/codegen/update/update_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"
#include "codegen/pipeline.h"
#include "codegen/runtime_state.h"
#include "planner/update_plan.h"
#include "codegen/table.h"

namespace peloton {
namespace codegen {

class UpdateTranslator : public OperatorTranslator {
 public:
  /** @brief Constructor. */
  UpdateTranslator(const planner::UpdatePlan &plan,
                   CompilationContext &context, Pipeline &pipeline);

  /** @brief Nothing to initialize. */
  void InitializeState() override {}

  /** @brief No helper functions. */
  void DefineAuxiliaryFunctions() override {}

  /** @brief Produce! */
  void Produce() const override;

  /** @brief Consume! */
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  /** @brief No state to tear down. */
  void TearDownState() override {}

  /** @brief Get the stringified name of this translator. */
  std::string GetName() const override { return "update"; }

 private:
  /** @brief The update plan. */
  const planner::UpdatePlan &update_plan_;

  TargetList target_list_;
  DirectMapList direct_list_;
  std::unique_ptr<Target[]> target_array;
  std::unique_ptr<DirectMap[]> direct_array;
  storage::DataTable *target_table_;
  bool update_primary_key_;
  RuntimeState::StateID target_val_vec_id_;
  RuntimeState::StateID col_id_vec_id_;

  // The code-generating table instance
  codegen::Table table_;
};  // class UpdateTranslator

}  // namespace codegen
}  // namespace peloton
