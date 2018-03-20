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
#include <array>

#include "codegen/codegen.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "codegen/oa_hash_table.h"
#include "codegen/runtime_state.h"
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
  // Constructor taking the runtime state reference
  Aggregation(RuntimeState &runtime_state) : runtime_state_(runtime_state) {}

  // Setup the aggregation to handle the provided aggregates
  void Setup(CodeGen &codegen,
             const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
             bool is_global, std::vector<type::Type> &grouping_ai_types);

  // Setup the aggregation to handle the provided aggregates
  void Setup(CodeGen &codegen,
             const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
             bool is_global);

  // Codegen any initialization work for the hash tables
  void InitializeState(CodeGen &codegen);

  // Cleanup by destroying the aggregation hash tables
  void TearDownState(CodeGen &codegen);

  // Create default initial values for all global aggregate components
  void CreateInitialGlobalValues(CodeGen &codegen, llvm::Value *space) const;

  // Store the provided values as the initial values for each of the aggregates
  void CreateInitialValues(
      CodeGen &codegen, llvm::Value *space,
      const std::vector<codegen::Value> &initial,
      const std::vector<codegen::Value> &grouping_keys) const;

  // Advance all stored aggregates (stored in the provided storage space) using
  // the values in the provided vector
  void AdvanceValues(CodeGen &codegen, llvm::Value *space,
                     const std::vector<codegen::Value> &next,
                     const std::vector<codegen::Value> &grouping_keys) const;

  // Advance all stored aggregates (stored in the provided storage space) using
  // the values in the provided vector
  void AdvanceValues(CodeGen &codegen, llvm::Value *space,
                     const std::vector<codegen::Value> &next) const;

  // Compute the final values of all the aggregates stored in the provided
  // storage space, inserting them into the provided output vector.
  void FinalizeValues(CodeGen &codegen, llvm::Value *space,
                      std::vector<codegen::Value> &final_vals) const;

  // Get the total number of bytes needed to store all the aggregates this is
  // configured to store
  uint32_t GetAggregatesStorageSize() const {
    return storage_.GetStorageSize();
  }

  // Get the storage format of the aggregates this class is configured to handle
  const UpdateableStorage &GetAggregateStorage() const { return storage_; }

 private:
  bool IsGlobal() const { return is_global_; }

  //===--------------------------------------------------------------------===//
  // Little struct to map higher level aggregates to their physical storage
  // and to their hash tables if they are distinct.
  //
  // Some aggregates decompose into multiple components. For example, AVG()
  // aggregates decompose into a SUM() and COUNT(). Therefore, the storage
  // indexes are stored in an array. The array has fixed size of the maximum
  // number of components that a aggregation is decomposed to, so for now
  // only 2 for AVG. The aggregations have to know which component is
  // stored at which index.
  //
  // Storing the mapping from the physical position the aggregate is stored to
  // where the caller expects them allows us to rearrange positions without
  // the caller knowing or caring.
  //===--------------------------------------------------------------------===//
  static const unsigned int kMaxNumComponents = 2;

  struct AggregateInfo {
    // The overall type of the aggregation
    const ExpressionType aggregate_type;

    // The position in the original (ordered) list of aggregates that this
    // aggregate is stored
    const uint32_t source_index;

    // This array contains the physical storage indices for the components the
    // aggregation is composed of.
    // The array is fixed-sized to the maximum possible length
    const std::array<uint32_t, kMaxNumComponents> storage_indices;

    // If the aggregate shall produce distinct output
    bool is_distinct;

    // Index for the runtime hash table, only used if is_distinct is true
    uint32_t hast_table_index;
  };

 private:
  void DoInitializeValue(CodeGen &codegen, llvm::Value *space,
                         ExpressionType type, uint32_t storage_index,
                         const Value &initial,
                         UpdateableStorage::NullBitmap &null_bitmap) const;

  // Will perform the NULL checking, update the null bitmap and call
  // DoAdvanceValue if appropriate
  void DoNullCheck(CodeGen &codegen, llvm::Value *space, ExpressionType type,
                   uint32_t storage_index, const codegen::Value &update,
                   UpdateableStorage::NullBitmap &null_bitmap) const;

  // Advance the value of a specific aggregate component, given its next value.
  // No NULL checking, the function assumes that the current aggregate value is
  // not NULL.
  void DoAdvanceValue(CodeGen &codegen, llvm::Value *space, ExpressionType type,
                      uint32_t storage_index, const codegen::Value &next) const;

  // Advancethe value of a specifig aggregate. Performs NULL check if necessary
  // and finally calls DoAdvanceValue()
  void AdvanceValue(CodeGen &codegen, llvm::Value *space,
                    const std::vector<codegen::Value> &next_vals,
                    const Aggregation::AggregateInfo &agg,
                    UpdateableStorage::NullBitmap &null_bitmap) const;

 private:
  // Is this a global aggregation?
  bool is_global_;

  // The list of aggregations we handle
  std::vector<AggregateInfo> aggregate_infos_;

  // The storage format we use to store values
  UpdateableStorage storage_;

  // Hash tables and their runtime IDs for the distinct aggregations, access via
  // index
  std::vector<std::pair<OAHashTable, RuntimeState::StateID>> hash_table_infos_;

  // Reference to RuntimeState, needed for the hash tables
  RuntimeState &runtime_state_;
};

}  // namespace codegen
}  // namespace peloton