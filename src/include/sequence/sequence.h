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
class Sequence {
 public:
  Sequence(const planner::CreatePlan &plan);

  Sequence(const std::string &name,
          const int64_t seqstart, const int64_t seqincrement,
          const int64_t seqmax, const int64_t seqmin,
          const int64_t seqcache, const bool seqcycle):
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
  int64_t seqstart;	// Start value of the sequence
  int64_t seqincrement;	// Increment value of the sequence
  int64_t seqmax;		// Maximum value of the sequence
  int64_t seqmin;		// Minimum value of the sequence
  int64_t seqcache;	// Cache size of the sequence
  bool seqcycle;	// Whether the sequence cycles

  int64_t GetNextVal();

 private:
  int64_t curr_val;
};


}  // namespace sequence
}  // namespace peloton
