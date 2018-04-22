//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oid_aggr_reducer.h
//
// Identification: src/statistics/oid_aggr_reducer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "stats_channel.h"
#include "function/functions.h"
#include <unordered_set>

namespace peloton {
namespace stats {

class OidAggrReducer {
 private:
  std::unordered_set<oid_t> &oid_set_;

 public:
  OidAggrReducer(std::unordered_set<oid_t> &oid_set) : oid_set_(oid_set) {}

  void inline Consume(oid_t oid) { oid_set_.insert(oid); }
};

}  // namespace stats
}  // namespace peloton