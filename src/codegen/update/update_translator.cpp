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
  throw new NotImplementedException("UpdateTranslator not implemented");
}

void UpdateTranslator::Consume(ConsumerContext &context,
                               RowBatch::Row &row) const {
  throw new NotImplementedException("UpdateTranslator not implemented");
}

}  // namespace codegen
}  // namespace peloton
