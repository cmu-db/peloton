//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.h
//
// Identification: src/include/codegen/expression_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/pipeline.h"
#include "planner/delete_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a delete operator
//===----------------------------------------------------------------------===//
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const planner::DeletePlan &delete_plan,
                   CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  void DefineAuxiliaryFunctions() override {}

  void TearDownState() override {}

  std::string GetName() const override { return "delete"; }

  void Produce() const override;

  void Consume(ConsumerContext &, RowBatch &) const override;
  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  const planner::DeletePlan &delete_plan_;
};


}  // namespace codegen
}  // namespace peloton
