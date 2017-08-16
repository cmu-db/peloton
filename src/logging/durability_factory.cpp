//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.cpp
//
// Identification: src/backend/logging/durability_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/durability_factory.h"
#include "logging/worker_context.h"
#include "concurrency/epoch_manager_factory.h"

namespace peloton {
namespace logging {

  LoggingType DurabilityFactory::logging_type_ = LoggingType::INVALID;

  CheckpointType DurabilityFactory::checkpoint_type_ = CHECKPOINT_TYPE_INVALID;

  TimerType DurabilityFactory::timer_type_ = TIMER_OFF;

  bool DurabilityFactory::generate_detailed_csv_ = false;


  //////////////////////////////////
  /*void DurabilityFactory::StartTxnTimer(size_t eid, WorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    uint64_t cur_time_usec = GetCurrentTimeInUsec();

    auto itr = worker_ctx->pending_txn_timers.find(eid);
    if (itr == worker_ctx->pending_txn_timers.end()) {
      itr = (worker_ctx->pending_txn_timers.emplace(eid, std::vector<uint64_t>())).first;
    }
    itr->second.emplace_back(cur_time_usec);
  }

  void DurabilityFactory::StopTimersByPepoch(size_t persist_eid, WorkerContext *worker_ctx) {
    if (DurabilityFactory::GetTimerType() == TIMER_OFF) return;

    if (persist_eid == worker_ctx->reported_eid) {
      return;
    }

//    uint64_t commit_time_usec = GetCurrentTimeInUsec();
    auto upper_itr = worker_ctx->pending_txn_timers.upper_bound(persist_eid);
    auto itr = worker_ctx->pending_txn_timers.begin();
    while (itr != upper_itr) {




      itr = worker_ctx->pending_txn_timers.erase(itr);
    }
    worker_ctx->reported_eid = persist_eid;
  }*/
}
}
