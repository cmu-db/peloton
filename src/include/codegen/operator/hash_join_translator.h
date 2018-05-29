//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_translator.h
//
// Identification: src/include/codegen/operator/hash_join_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/bloom_filter_accessor.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/hash_table.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/updateable_storage.h"

namespace peloton {

namespace planner {
class HashJoinPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a hash-join operator
//===----------------------------------------------------------------------===//
class HashJoinTranslator : public OperatorTranslator {
 public:
  // Global/configurable variable controlling whether hash aggregations prefetch
  static std::atomic<bool> kUsePrefetch;

  HashJoinTranslator(const planner::HashJoinPlan &join,
                     CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch &batch) const override;
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void RegisterPipelineState(PipelineContext &pipeline_ctx) override;
  void InitializePipelineState(PipelineContext &pipeline_ctx) override;
  void TearDownPipelineState(PipelineContext &pipeline_ctx) override;
  void FinishPipeline(PipelineContext &pipeline_ctx) override;

  // Codegen any cleanup work for this translator
  void TearDownQueryState() override;

 private:
  // Consume the given context from the left/build side or the right/probe side
  void ConsumeFromLeft(ConsumerContext &context, RowBatch::Row &row) const;
  void ConsumeFromRight(ConsumerContext &context, RowBatch::Row &row) const;

  bool IsLeftPipeline(const Pipeline &pipeline) const {
    return pipeline == left_pipeline_;
  }

  bool IsFromLeftChild(ConsumerContext &context) const {
    return IsLeftPipeline(context.GetPipeline());
  }

  void CollectKeys(
      RowBatch::Row &row,
      const std::vector<const expression::AbstractExpression *> &key,
      std::vector<codegen::Value> &key_values) const;

  void CollectValues(RowBatch::Row &row,
                     const std::vector<const planner::AttributeInfo *> &ais,
                     std::vector<codegen::Value> &values) const;

  void CodegenHashProbe(ConsumerContext &context, RowBatch::Row &row,
                        std::vector<codegen::Value> &key) const;

  /// Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  /// Return the estimated number of tuples produced by the left child
  uint64_t EstimateCardinalityLeft() const;

  /// Should this operator employ prefetching?
  bool UsePrefetching() const;

  const planner::HashJoinPlan &GetJoinPlan() const;

 private:
  /// Handy classes

  /// Callback used for probing the hash table
  class ProbeRight;

  /// Callback used when inserting a tuple in the hash table during build
  class InsertLeft;

 private:
  // The build-side pipeline
  Pipeline left_pipeline_;

  // The ID of the hash-table in the runtime state
  QueryState::Id hash_table_id_;
  PipelineContext::Id hash_table_tl_id_;

  // The ID of the bloom filter in the runtime state
  QueryState::Id bloom_filter_id_;

  // The hash table we use to perform the join
  HashTable hash_table_;

  // Bloom Filter Accessor
  BloomFilterAccessor bloom_filter_;

  // The left and right hash key expressions
  std::vector<const expression::AbstractExpression *> left_key_exprs_;
  std::vector<const expression::AbstractExpression *> right_key_exprs_;

  // The (unique) set of build-side attributes that are materialized
  std::vector<const planner::AttributeInfo *> left_val_ais_;

  // The storage format used to store build-attributes in hash-table
  CompactStorage left_value_storage_;

  // Does this join need an output vector
  bool needs_output_vector_;
};

}  // namespace codegen
}  // namespace peloton