//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_translator.h
//
// Identification: src/include/codegen/operator/projection_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/operator/operator_translator.h"
#include "codegen/pipeline.h"

namespace peloton {

namespace planner {
class ProjectInfo;
class ProjectionPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for projections.
//===----------------------------------------------------------------------===//
class ProjectionTranslator : public OperatorTranslator {
 public:
  // Constructor
  ProjectionTranslator(const planner::ProjectionPlan &plan,
                       CompilationContext &context, Pipeline &pipeline);

  // Nothing to initialize
  void InitializeState() override {}

  // No helper functions
  void DefineAuxiliaryFunctions() override {}

  // Produce!
  void Produce() const override;

  // Consume!
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // No state to tear down
  void TearDownState() override {}

  // Get the stringified name of this translator
  std::string GetName() const override;

  // Helpers
  static void PrepareProjection(CompilationContext &context,
                                const planner::ProjectInfo &projection_info);

  static void AddNonTrivialAttributes(
      RowBatch &row_batch, const planner::ProjectInfo &projection_info,
      std::vector<RowBatch::ExpressionAccess> &accessors);

 private:
  // The aggregation plan
  const planner::ProjectionPlan &plan_;
};

}  // namespace codegen
}  // namespace peloton