//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence.h
//
// Identification: src/include/sequence/sequence.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "common/logger.h"
#include "planner/create_plan.h"
#include "storage/tuple.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace sequence {
class Sequence {
 public:
  Sequence(const planner::CreatePlan &plan);

  Sequence(const std::string &name,
          const int64_t seqstart, const int64_t seqincrement,
          const int64_t seqmax, const int64_t seqmin,
          const bool seqcycle, const int64_t seqval):
          seq_name(name),
          seq_start(seqstart),
          seq_increment(seqincrement),
          seq_max(seqmax),
          seq_min(seqmin),
          seq_cycle(seqcycle),
          seq_curr_val(seqval){};

  std::string seq_name;
  int64_t seq_start;	// Start value of the sequence
  int64_t seq_increment;	// Increment value of the sequence
  int64_t seq_max;		// Maximum value of the sequence
  int64_t seq_min;		// Minimum value of the sequence
  int64_t seq_cache;	// Cache size of the sequence
  bool seq_cycle;	// Whether the sequence cycles

  std::mutex sequence_mutex; // mutex for all operations
  int64_t GetNextVal();

  int64_t GetCurrVal() { return seq_curr_val; }; // only visible for test!
  void SetCurrVal(int64_t curr_val) { seq_curr_val = curr_val; }; // only visible for test!
  void SetCycle(bool cycle) { seq_cycle = cycle; };

 private:
  int64_t seq_curr_val;
  int64_t get_next_val();
};


}  // namespace sequence
}  // namespace peloton
