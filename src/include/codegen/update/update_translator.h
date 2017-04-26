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
#include "planner/update_plan.h"

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

};  // class UpdateTranslator

}  // namespace codegen
}  // namespace peloton
