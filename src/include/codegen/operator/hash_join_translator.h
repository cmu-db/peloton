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
#include "codegen/oa_hash_table.h"
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

  // Codegen any initialization work for this operator
  void InitializeState() override;

  // Define any helper functions this translator needs
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumes tuples from child operators
  void Consume(ConsumerContext &context, RowBatch &batch) const override;
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // Codegen any cleanup work for this translator
  void TearDownState() override;

  std::string GetName() const override;

 private:
  // Consume the given context from the left/build side or the right/probe side
  void ConsumeFromLeft(ConsumerContext &context, RowBatch::Row &row) const;
  void ConsumeFromRight(ConsumerContext &context, RowBatch::Row &row) const;

  bool IsFromLeftChild(ConsumerContext &context) const {
    return context.GetPipeline().GetChild() == left_pipeline_.GetChild();
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

  // Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  // Return the estimated number of tuples produced by the left child
  uint64_t EstimateCardinalityLeft() const;

  // Should this operator employ prefetching?
  bool UsePrefetching() const;

  const planner::HashJoinPlan &GetJoinPlan() const { return join_; }

  //===--------------------------------------------------------------------===//
  // The callback used when we probe the hash table with right-side tuples
  // during the probe phase of the join
  //===--------------------------------------------------------------------===//
  class ProbeRight : public OAHashTable::IterateCallback {
   public:
    // Constructor
    ProbeRight(const HashJoinTranslator &join_translator,
               ConsumerContext &context, RowBatch::Row &row,
               const std::vector<codegen::Value> &right_key);

    // Process the given key and associated data area
    void ProcessEntry(CodeGen &codegen, const std::vector<codegen::Value> &key,
                      llvm::Value *data_area) const override;

   private:
    // The translator (we need lots of its state)
    const HashJoinTranslator &join_translator_;
    // The compilation context
    ConsumerContext &context_;
    RowBatch::Row &row_;
    const std::vector<codegen::Value> &right_key_;
  };

  //===--------------------------------------------------------------------===//
  // The callback used during build phase to materialize the left input tuple
  // into the hash table
  //===--------------------------------------------------------------------===//
  class InsertLeft : public OAHashTable::InsertCallback {
   public:
    // Constructor
    InsertLeft(const CompactStorage &storage,
               const std::vector<codegen::Value> &values);
    // StoreValue the input tuple in the given data space
    void StoreValue(CodeGen &codegen, llvm::Value *data_space) const override;
    llvm::Value *GetValueSize(CodeGen &codegen) const override;

   private:
    // The storage format of the values in the hash table
    const CompactStorage storage_;
    // The attribute values from the left side
    const std::vector<codegen::Value> &values_;
  };

 private:
  // The hash join plan node that contains all the information we need
  const planner::HashJoinPlan &join_;

  // The build-side pipeline
  Pipeline left_pipeline_;

  // The ID of the hash-table in the runtime state
  RuntimeState::StateID hash_table_id_;

  // The ID of the bloom filter in the runtime state
  RuntimeState::StateID bloom_filter_id_;

  // The hash table we use to perform the join
  OAHashTable hash_table_;

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