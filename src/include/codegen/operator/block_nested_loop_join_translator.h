//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// block_nested_loop_join_translator.h
//
// Identification:
// src/include/codegen/operator/block_nested_loop_join_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/auxiliary_producer_function.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/sorter.h"

namespace peloton {

namespace planner {
class NestedLoopJoinPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a blockwise nested loop join
//===----------------------------------------------------------------------===//
class BlockNestedLoopJoinTranslator : public OperatorTranslator {
 public:
  BlockNestedLoopJoinTranslator(const planner::NestedLoopJoinPlan &nlj_plan,
                                CompilationContext &context,
                                Pipeline &pipeline);

  void InitializeState() override;

  void DefineAuxiliaryFunctions() override;

  void TearDownState() override;

  std::string GetName() const override;

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

 private:
  bool IsFromLeftChild(const Pipeline &pipeline) const;

  void ConsumeFromLeft(ConsumerContext &context, RowBatch::Row &row) const;
  void ConsumeFromRight(ConsumerContext &context, RowBatch::Row &row) const;

  void FindMatchesForRow(ConsumerContext &ctx, RowBatch::Row &row) const;

  const planner::NestedLoopJoinPlan &GetPlan() const { return nlj_plan_; }

 private:
  // The plan
  const planner::NestedLoopJoinPlan &nlj_plan_;

  // The pipeline for the left subtree of the plan
  Pipeline left_pipeline_;

  // All the attributes from the left input that are materialized
  std::vector<const planner::AttributeInfo *> unique_left_attributes_;

  // The memory space we use to buffer left input tuples. We use a util::Sorter
  // instance because it provides a simple API to append tuples into a buffer.
  // We **DO NOT** actually sort the input at all.
  RuntimeState::StateID buffer_id_;
  Sorter buffer_;

  // This controls the number of tuples we buffer before performing the nested
  // loop join. Ideally, we want the buffer to always be, at least, L2 cache
  // resident. Knowing the tuple layout coming from the left child, we calculate
  // this value accurately.
  uint32_t max_buf_rows_;

  // This is the function called when enough tuples from the left side have been
  // buffered and we want to perform the BNLJ against the right input.
  AuxiliaryProducerFunction join_buffer_func_;
};

}  // namespace codegen
}  // namespace peloton
