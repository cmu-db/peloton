//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.h
//
// Identification: src/include/codegen/hash_group_by_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator_translator.h"
#include "codegen/updateable_storage.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a hash-based group-by operator.
//===----------------------------------------------------------------------===//
class HashGroupByTranslator : public OperatorTranslator {
 public:
  // Global/configurable variable controlling whether hash aggregations prefetch
  static std::atomic<bool> kUsePrefetch ;

  // Constructor
  HashGroupByTranslator(const planner::AggregatePlan &group_by,
                        CompilationContext &context, Pipeline &pipeline);

  // Codegen any initialization work for this operator
  void InitializeState() override;

  // Define any helper functions this translator needs
  void DefineFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumes tuples from child operators
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;
  void Consume(ConsumerContext &context, RowBatch &batch) const override;

  // Codegen any cleanup work for this translator
  void TearDownState() override;

  // Get a stringified name for this hash-table based aggregation
  std::string GetName() const override;

 private:
  //===--------------------------------------------------------------------===//
  // The callback the group-by uses when iterating the results of the hash table
  //===--------------------------------------------------------------------===//
  class ProduceResults : public HashTable::VectorizedIterateCallback {
   public:
    // Constructor
    ProduceResults(const planner::AggregatePlan &gb,
                   const Aggregation &aggregation, CompilationContext &context,
                   Pipeline &pipeline);

    // The callback
    void ProcessEntries(CodeGen &codegen, llvm::Value *start, llvm::Value *end,
                        Vector &selection_vector,
                        HashTable::HashTableAccess &access) const override;

   private:
    // The plan details
    const planner::AggregatePlan &group_by_;
    // The storage format of the values in the hash table
    const Aggregation &aggregation_;
    // The context we populate
    CompilationContext &compilation_ctx_;
    // The pipeline the group by is a part of
    Pipeline &pipeline_;
  };

  //===--------------------------------------------------------------------===//
  // The callback used when we probe the hash table when aggregating, but find
  // an existing value associated with a given key. We, therefore, perform the
  // actual aggregation ... duh
  //===--------------------------------------------------------------------===//
  class ConsumerProbe : public HashTable::ProbeCallback {
   public:
    // Constructor
    ConsumerProbe(const Aggregation &aggregation,
                  const std::vector<codegen::Value> &next_vals);

    // The callback
    void ProcessEntry(CodeGen &codegen, llvm::Value *data_area) const override;

   private:
    // The guy that handles the computation of the aggregates
    const Aggregation &aggregation_;
    // The next value to merge into the existing aggregates
    const std::vector<codegen::Value> &next_vals_;
  };

  //===--------------------------------------------------------------------===//
  // The callback used when we probe the hash table when aggregating, but do
  // not find an existing entry. At this point, we insert the values as initial
  // aggregates.
  //===--------------------------------------------------------------------===//
  class ConsumerInsert : public HashTable::InsertCallback {
   public:
    // Constructor
    ConsumerInsert(const Aggregation &aggregation,
                   const std::vector<codegen::Value> &initial_vals);

    // Store the initial values of the aggregates into the provided storage
    void Store(CodeGen &codegen, llvm::Value *data_space) const override;

    llvm::Value *PayloadSize(CodeGen &codegen) const override;

   private:
    // The guy that handles the computation of the aggregates
    const Aggregation &aggregation_;
    // The list of initial values to use as aggregates
    const std::vector<codegen::Value> &initial_vals_;
  };

  //===--------------------------------------------------------------------===//
  // An aggregate finalizer allows aggregations to delay the finalization of an
  // aggregate in the hash-table to a later time. This is needed when we do
  // vectorized scans of the hash-table.
  //===--------------------------------------------------------------------===//
  class AggregateFinalizer {
   public:
    AggregateFinalizer(const Aggregation &aggregation,
                       HashTable::HashTableAccess &hash_table_access);
    // Get the finalized aggregates at the given position in the results
    const std::vector<codegen::Value> &GetAggregates(CodeGen &codegen,
                                                     llvm::Value *index);

   private:
    // The aggregator
    Aggregation aggregation_;
    // The hash-table accesor
    HashTable::HashTableAccess &hash_table_access_;
    // Whether the aggregate has been finalized and the results
    bool finalized_;
    std::vector<codegen::Value> final_aggregates_;
  };

  //===--------------------------------------------------------------------===//
  // This class provides (delayed) access to individual attributes of tuples in
  // the result of the aggregation.
  //===--------------------------------------------------------------------===//
  class AggregateAccess : public RowBatch::AttributeAccess {
   public:
    AggregateAccess(AggregateFinalizer &finalizer, uint32_t agg_index);

    codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override;

   private:
    // The associate finalizer
    AggregateFinalizer &finalizer_;
    // The index in the tuple's attributes
    uint32_t agg_index_;
  };

  void CollectHashKeys(ConsumerContext &context, RowBatch::Row &row,
                       std::vector<codegen::Value> &key) const;

  // Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  // Should this operator employ prefetching?
  bool UsePrefetching() const;

 private:
  // The group-by plan
  const planner::AggregatePlan &group_by_;

  // The pipeline forming all child operators of this aggregation
  Pipeline child_pipeline_;

  // The ID of the hash-table in the runtime state
  RuntimeState::StateID hash_table_id_;

  // The hash table
  OAHashTable hash_table_;

  // The ID of the output vector (for vectorized result production)
  RuntimeState::StateID output_vector_id_;

  // The ID of the group-prefetch vector, if we're prefetching
  RuntimeState::StateID prefetch_vector_id_;

  // The aggregation handler
  Aggregation aggregation_;
};

}  // namespace codegen
}  // namespace peloton