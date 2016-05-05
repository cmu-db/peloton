//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_stats_context.cpp
//
// Identification: src/backend/statistics/backend_stats_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <mutex>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/statistics/backend_stats_context.h"
#include "backend/statistics/stats_aggregator.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


BackendStatsContext::BackendStatsContext() {
  std::thread::id this_id = std::this_thread::get_id();
  thread_id = this_id;


}
BackendStatsContext::~BackendStatsContext() {
  //peloton::stats::StatsAggregator::GetInstance().UnregisterContext(thread_id);

}

void BackendStatsContext::Aggregtate(BackendStatsContext &source_) {
  txn_committed.Aggregate(source_.txn_committed);
  txn_aborted.Aggregate(source_.txn_aborted);
}


}  // namespace stats
}  // namespace peloton
