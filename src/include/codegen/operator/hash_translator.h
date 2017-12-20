//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_translator.h
//
// Identification: src/include/codegen/operator/hash_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/updateable_storage.h"

namespace peloton {

namespace planner {
class HashPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a hash-based distinct operator.
//===----------------------------------------------------------------------===//
class HashTranslator : public OperatorTranslator {
 public:
  // Constructor
  HashTranslator(const planner::HashPlan &hash_plan,
                 CompilationContext &context, Pipeline &pipeline);

  // Codegen any initialization work for this operator
  void InitializeState() override;

  // Define any helper functions this translator needs
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumes tuples from child operators
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // Codegen any cleanup work for this translator
  void TearDownState() override;

  // Get a stringified name for this hash-table based aggregation
  std::string GetName() const override;

 private:
  void CollectHashKeys(RowBatch::Row &row,
                       std::vector<codegen::Value> &key) const;

  // Estimate the size of the constructed hash table
  uint64_t EstimateHashTableSize() const;

  const planner::HashPlan &GetHashPlan() const { return hash_plan_; }

 private:
  //===--------------------------------------------------------------------===//
  // The callback used when we probe the hash table and the key already exists.
  // It's a dummy class that just drops the row and does nothing.
  //===--------------------------------------------------------------------===//
  class ConsumerProbe : public HashTable::ProbeCallback {
   public:
    // The callback
    void ProcessEntry(CodeGen &codegen, llvm::Value *data_area) const override;
  };

  //===--------------------------------------------------------------------===//
  // The callback used when we probe the hash table when aggregating, but do
  // not find an existing entry. It passes the row on to the parent.
  //===--------------------------------------------------------------------===//
  class ConsumerInsert : public HashTable::InsertCallback {
   public:
    // Constructor
    explicit ConsumerInsert(ConsumerContext &context, RowBatch::Row &row);

    // StoreValue the initial values of the aggregates into the provided storage
    void StoreValue(CodeGen &codegen, llvm::Value *data_space) const override;

    llvm::Value *GetValueSize(CodeGen &codegen) const override;

   private:
    // ConsumerContext on which the consume will be called
    ConsumerContext &context_;

    // The row that will be given to the parent
    RowBatch::Row &row_;
  };

  // The hash plan
  const planner::HashPlan &hash_plan_;

  // The ID of the hash-table in the runtime state
  RuntimeState::StateID hash_table_id_;

  // The hash table
  OAHashTable hash_table_;
};

}  // namespace codegen
}  // namespace peloton