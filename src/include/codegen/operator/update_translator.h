//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.h
//
// Identification: src/include/codegen/update_translator.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/pipeline.h"
#include "codegen/query_state.h"
#include "codegen/table_storage.h"

namespace peloton {

namespace planner {
class UpdatePlan;
}  // namespace planner

namespace codegen {

class UpdateTranslator : public OperatorTranslator {
 public:
  // Constructor
  UpdateTranslator(const planner::UpdatePlan &plan, CompilationContext &context,
                   Pipeline &pipeline);

  void InitializeQueryState() override;

  // No helper functions
  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownQueryState() override;

 private:
  // Target table
  storage::DataTable *target_table_;

  // Runtime state id for the updater
  QueryState::Id updater_state_id_;

  // Tuple storage area
  codegen::TableStorage table_storage_;
};

}  // namespace codegen
}  // namespace peloton
