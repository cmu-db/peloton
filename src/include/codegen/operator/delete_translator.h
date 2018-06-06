//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_translator.h
//
// Identification: src/include/codegen/operator/delete_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/operator/operator_translator.h"
#include "codegen/table.h"

namespace peloton {

namespace planner {
class DeletePlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a delete operator
//===----------------------------------------------------------------------===//
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const planner::DeletePlan &delete_plan,
                   CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override {}

  void TearDownQueryState() override {}

  void Produce() const override;

  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  // Table accessor
  codegen::Table table_;

  // The Deleter instance
  QueryState::Id deleter_state_id_;
};

}  // namespace codegen
}  // namespace peloton
