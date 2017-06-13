//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.h
//
// Identification: src/include/codegen/aggregation.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class is responsible for handling the logic around performing
// aggregations.  We setup/initialize it with the aggregations we want to
// handle, then invoke the CreateInitialValues() function to store the initial
// aggregate values into the provided storage space.  Subsequent calls to
// AdvanceValues(...) will generate the code to advance each of the aggregates
// and store them in the correct location in the provided storage space.
//
// It is important to note that the ordering of aggregates must be consistent
// for all uses of an Aggregation instance after Setup(). This means that if a
// SUM(..) aggregate exists at position four during Setup(..), then position
// four of the vector provided to AdvanceValues(..) should contain the value
// for how far to advance the summation.
//===----------------------------------------------------------------------===//
class Aggregation {
 public:
  // Setup this guy to handle the aggregates in the provided vector
  void Setup(CodeGen &codegen,
             const std::vector<planner::AggregatePlan::AggTerm> &agg_terms);

  // StoreValue the initial values for all aggregates into the provided storage
  // space. The size of the storage space _must_ be at least as the return
  // value of GetAggregatesStorageSize().
  void CreateInitialValues(CodeGen &codegen, llvm::Value *storage_space,
                           const std::vector<codegen::Value> &initial) const;

  // Advance all the aggregates that are stored in the provided storage space
  // by the values from the provided vector.
  void AdvanceValues(CodeGen &codegen, llvm::Value *storage_space,
                     const std::vector<codegen::Value> &next) const;

  // Compute the final values of all the aggregates stored in the provided
  // storage space, putting them into the final_vals vector
  void FinalizeValues(CodeGen &codegen, llvm::Value *storage_space,
                      std::vector<codegen::Value> &final_vals) const;

  // Get the total number of bytes needed to store all the aggregates this is
  // configured to store
  uint32_t GetAggregatesStorageSize() const {
    return storage_.GetStorageSize();
  }

  // Get the storage format of the aggregates this class is configured to handle
  const UpdateableStorage &GetAggregateStorage() const { return storage_; }

 private:
  //===--------------------------------------------------------------------===//
  // Little struct to map the aggregates we physically store to the higher level
  // aggregates. It is possible that the number of aggregate information structs
  // we keep is not equal to the total number of aggregates the caller has
  // setup. This can occur for two reasons:
  //
  // 1) There are occasions where components of aggregates can be shared
  //    across multiple aggregates.  An example is a SUM(a) and AVG(a). Both
  //    of these will share the summation on the column.
  // 2) Some aggregates decompose into simpler aggregations. An example is AVG()
  //    which we decompose into a SUM() and COUNT().  AVG(), therefore, occupies
  //    three total slots.
  //
  // Storing the mapping from the physical position the aggregate is stored to
  // where the caller expects them allows us to rearrange positions without
  // the caller knowing or caring.
  //===--------------------------------------------------------------------===//
  struct AggregateInfo {
    // The type of aggregate
    ExpressionType aggregate_type;

    // The SQL (data) type of the aggregate
    type::TypeId type;

    // The position in the original (ordered) list of aggregates that this
    // aggregate is stored
    uint32_t source_index;

    // The position in the physical storage space where this aggregate is stored
    uint32_t storage_index;

    // Is this internal? In other words, does the caller know that this
    // aggregate exists?
    bool is_internal;
  };

 private:
  // Advance the value of a specific aggregate, given its next value
  void DoAdvanceValue(CodeGen &codegen, llvm::Value *storage_space,
                      const AggregateInfo &aggregate_info,
                      const codegen::Value &next) const;

 private:
  // The list of aggregations we handle
  std::vector<AggregateInfo> aggregate_infos_;

  // The storage format we use to store values
  UpdateableStorage storage_;
};

}  // namespace codegen
}  // namespace peloton