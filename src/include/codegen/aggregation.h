//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.h
//
// Identification: src/include/codegen/aggregation.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "codegen/compilation_context.h"
#include "codegen/oa_hash_table.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class is responsible for handling the logic around performing
// aggregations. Users first setup the aggregation (through Setup()) with all
// the aggregates they wish calculate. Next, callers provided the initial values
// of all the aggregates using a call to CreateInitialValues(). Each update to
// the set of aggregates is made through AdvanceValues(), with updated values
// for each aggregate. When done, a final call to FinalizeValues() is made to
// collect all the final aggregate values.
//
// Note: the ordering of aggregates and values must be consistent with the
//       ordering provided during Setup().
//===----------------------------------------------------------------------===//
class Aggregation {
 public:
  // Setup the aggregation to handle the provided aggregates
  void Setup (CompilationContext &context,
              const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
              bool is_global, std::vector<type::Type> &grouping_ai_types);

  // Setup the aggregation to handle the provided aggregates
  void Setup (CompilationContext &context,
              const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
              bool is_global);

  // Codegen any initialization work for the hash tables
  void InitializeState (CompilationContext &context);

  // Cleanup by destroying the aggregation hash tables
  void TearDownState (CompilationContext &context);

  // Create default initial values for all global aggregate components
  void CreateInitialGlobalValues (CodeGen &codegen, llvm::Value *space) const;

  // Store the provided values as the initial values for each of the aggregates
  void CreateInitialValues (CompilationContext &context,
                            llvm::Value *space,
                            const std::vector<codegen::Value> &initial,
                            const std::vector<codegen::Value> &grouping_keys) const;

  // Advance all stored aggregates (stored in the provided storage space) using
  // the values in the provided vector
  void AdvanceValues (CompilationContext &context, llvm::Value *space,
                      const std::vector<codegen::Value> &next,
                      const std::vector<codegen::Value> &grouping_keys) const;

  // Advance all stored aggregates (stored in the provided storage space) using
  // the values in the provided vector
  void AdvanceValues (CompilationContext &context, llvm::Value *space,
                      const std::vector<codegen::Value> &next) const;

  // Compute the final values of all the aggregates stored in the provided
  // storage space, inserting them into the provided output vector.
  void FinalizeValues (CodeGen &codegen, llvm::Value *space,
                       std::vector<codegen::Value> &final_vals) const;

  // Get the total number of bytes needed to store all the aggregates this is
  // configured to store
  uint32_t GetAggregatesStorageSize () const {
    return storage_.GetStorageSize();
  }

  // Get the storage format of the aggregates this class is configured to handle
  const UpdateableStorage &GetAggregateStorage () const { return storage_; }

 private:
  bool IsGlobal () const { return is_global_; }

  //===--------------------------------------------------------------------===//
  // Little struct to map the aggregates we physically store to the higher level
  // aggregates. It is possible that the number of these structs is greater than
  // the total number of aggregates the caller has setup. This can occur for two
  // reasons:
  //
  // 1) Some aggregates decompose into multiple aggregations. For example, AVG()
  //    aggregates decompose into a SUM() and COUNT(), therefore occupying three
  //    slots: one each for the sum, count, and logical average.
  // 2) There are occasions where components of aggregates can be shared across
  //    multiple aggregates.  An example is a SUM(a) and AVG(a). Both of these
  //    will share the summation aggregate on 'a'.
  //
  // Storing the mapping from the physical position the aggregate is stored to
  // where the caller expects them allows us to rearrange positions without
  // the caller knowing or caring.
  //===--------------------------------------------------------------------===//
  struct AggregateInfo {
    // The type of aggregate
    ExpressionType aggregate_type;

    // The data type of the aggregate
    const type::Type type;

    // The position in the original (ordered) list of aggregates that this
    // aggregate is stored
    uint32_t source_index;

    // The position in the physical storage space where this aggregate is stored
    uint32_t storage_index;

    // Is this aggregate purely for internal use? Is this externally visible?
    bool is_internal;

    // If the aggregate shall produce distinct output
    bool is_distinct;

    // Index for the runtime hash table, only used if is_distinct is true
    // Exception: AVG is a internal aggregation that can be distinct,
    // but doesn't have a hash table
    uint32_t hast_table_index;
  };

 private:
  // Will perform the NULL checking, update the null bitmap and call DoAdvanceValue
  // if appropriate
  void DoNullCheck (CodeGen &codegen,
                    llvm::Value *space,
                    const Aggregation::AggregateInfo &aggregate_info,
                    const codegen::Value &update,
                    UpdateableStorage::NullBitmap &null_bitmap,
                    llvm::Value *curr_val = nullptr) const;

  // Advance the value of a specific aggregate, given its next value without any
  // NULL checking. This assumes that the current aggregate value is not NULL.
  void DoAdvanceValue (CodeGen &codegen, llvm::Value *space,
                       const AggregateInfo &agg_info,
                       const codegen::Value &next) const;

 private:
  // Is this a global aggregation?
  bool is_global_;

  // The list of aggregations we handle
  std::vector<AggregateInfo> aggregate_infos_;

  // The storage format we use to store values
  UpdateableStorage storage_;

  // Hash tables and their runtime IDs for the distinct aggregations, access via index
  std::vector<std::pair<OAHashTable, RuntimeState::StateID>> hash_table_infos_;
};

}  // namespace codegen
}  // namespace peloton