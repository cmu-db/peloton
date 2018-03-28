//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence.cpp
//
// Identification: src/sequence/sequence.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/logger.h"
#include "planner/create_plan.h"
#include "storage/tuple.h"
#include "common/internal_types.h"
#include "sequence/sequence.h"

namespace peloton {
namespace sequence {
  Sequence::Sequence(const planner::CreatePlan &plan) {
    seq_name = plan.GetSequenceName();
    seqstart = plan.GetSequenceStart();	// Start value of the sequence
    seqincrement = plan.GetSequenceIncrement();	// Increment value of the sequence
    seqmax = plan.GetSequenceMaxValue();		// Maximum value of the sequence
    seqmin = plan.GetSequenceMinValue();		// Minimum value of the sequence
    seqcache = plan.GetSequenceCacheSize();	// Cache size of the sequence
    seqcycle = plan.GetSequenceCycle();	// Whether the sequence cycles

    curr_val = seqstart;
  }

  int64_t GetNextVal(){
    return 0;
  }

}  // namespace sequence
}  // namespace peloton
