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

#include "common/logger.h"
#include "planner/create_plan.h"
#include "storage/tuple.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace sequence {

class Sequence;

class SequenceData {
 public:
  int64 last_value;
  int64 log_cnt;
  bool is_called;
};

class Sequence {
 public:
  Sequence(const planner::CreatePlan &plan);

  Sequence(const std::string &name,
          const int64 seqstart, const int64 seqincrement,
          const int64 seqmax, const int64 seqmin,
          const int64 seqcache, const bool seqcycle):
          seq_name(name),
          seqstart(seqstart),
          seqincrement(seqincrement),
          seqmax(seqmax),
          seqmin(seqmin),
          seqcache(seqcache),
          seqcycle(seqcycle)
          {
            curr_val = seqstart;
          }

  std::string seq_name;
  int64 seqstart;	// Start value of the sequence
  int64 seqincrement;	// Increment value of the sequence
  int64 seqmax;		// Maximum value of the sequence
  int64 seqmin;		// Minimum value of the sequence
  int64 seqcache;	// Cache size of the sequence
  bool seqcycle;	// Whether the sequence cycles

  int64 GetNextVal();

 private:
  int64 curr_val;
};


}  // namespace sequence
}  // namespace peloton
