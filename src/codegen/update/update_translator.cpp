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

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), update_plan_(update_plan) {

  // Also create the translator for our child.
  context.Prepare(*update_plan.GetChild(0), pipeline);
}

void UpdateTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();
  compilation_context.Produce(*this->update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &context,
                               RowBatch::Row &row) const {
  // The transformation.
  auto *project_info = this->update_plan_.GetProjectInfo();
  auto &codegen = this->GetCodeGen();

  (void)context;
  (void)row;
  (void)project_info;
  (void)codegen;

  throw new NotImplementedException("UpdateTranslator not implemented");
}

}  // namespace codegen
}  // namespace peloton
