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

#include <mutex>

#include "common/logger.h"
#include "planner/create_plan.h"
#include "storage/tuple.h"
#include "common/internal_types.h"
#include "sequence/sequence.h"
#include "common/exception.h"
#include "util/string_util.h"

namespace peloton {
namespace sequence {

Sequence::Sequence(const planner::CreatePlan &plan) {
  seq_name = plan.GetSequenceName();
  seq_start = plan.GetSequenceStart();	// Start value of the sequence
  seq_increment = plan.GetSequenceIncrement();	// Increment value of the sequence
  seq_max = plan.GetSequenceMaxValue();		// Maximum value of the sequence
  seq_min = plan.GetSequenceMinValue();		// Minimum value of the sequence
  seq_cycle = plan.GetSequenceCycle();	// Whether the sequence cycles

  seq_curr_val = seq_start;
}

int64_t Sequence::GetNextVal() {
  std::lock_guard<std::mutex> lock(sequence_mutex);
  return get_next_val();
}

int64_t Sequence::get_next_val() {
  int64_t result = seq_curr_val;
  if(seq_increment > 0) {
    if ((seq_max >= 0 && seq_curr_val > seq_max - seq_increment) ||
      (seq_max < 0 && seq_curr_val + seq_increment > seq_max)){
        if (!seq_cycle) {
          throw SequenceException(StringUtil::Format("Sequence exceeds upper limit!"));
        }
        seq_curr_val = seq_min;
      }
    else
      seq_curr_val += seq_increment;
  }
  else {
    if ((seq_min < 0 && seq_curr_val < seq_min - seq_increment) ||
      (seq_min >= 0 && seq_curr_val + seq_increment < seq_min))
    {
      if (!seq_cycle){
          throw SequenceException(StringUtil::Format("Sequence exceeds lower limit!"));
      }
      seq_curr_val = seq_max;
    }
    else
      seq_curr_val += seq_increment;
  }
  return result;
}

}  // namespace sequence
}  // namespace peloton
