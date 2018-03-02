//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator.h
//
// Identification: src/include/codegen/operator/order_by_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/sorter.h"
#include "codegen/updateable_storage.h"

namespace peloton {

namespace planner {
class OrderByPlan;
}  // namespace planner

namespace codegen {

/**
 * Translator for sorting/order-by operators.
 */
class OrderByTranslator : public OperatorTranslator {
 public:
  OrderByTranslator(const planner::OrderByPlan &plan,
                    CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;
  void TearDownQueryState() override;

  // We parallel, son
  void RegisterPipelineState(PipelineContext &pipeline_ctx) override;
  void InitializePipelineState(PipelineContext &pipeline_ctx) override;
  void FinishPipeline(PipelineContext &pipeline_ctx) override;
  void TearDownPipelineState(PipelineContext &pipeline_ctx) override;

  void DefineAuxiliaryFunctions() override;

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

 private:
  // Helper class declarations
  class ProduceResults;
  class SorterAttributeAccess;

 private:
  // The child pipeline
  Pipeline child_pipeline_;

  // The ID of our sorter instance in the runtime state
  QueryState::Id sorter_id_;
  PipelineContext::Id thread_sorter_id_;

  // The sorter translator instance
  Sorter sorter_;

  // The (generated) comparison function
  llvm::Function *compare_func_;

  struct SortKeyInfo {
    // The sort key
    const planner::AttributeInfo *sort_key;

    // Is the sort key materialized in the tuple?
    bool is_part_of_output;

    // The slot in the materialized tuple to find the sort key column
    uint32_t tuple_slot;
  };

  std::vector<SortKeyInfo> sort_key_info_;
};

}  // namespace codegen
}  // namespace peloton