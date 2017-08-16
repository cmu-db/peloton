//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.h
//
// Identification: src/backend/logging/durability_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sys/time.h>

#include "logging/dummy_log_manager.h"
#include "logging/phylog_checkpoint_manager.h"
#include "logging/dummy_checkpoint_manager.h"
#include "logging/reordered_phylog_log_manager.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:

  static ReorderedPhyLogLogManager& GetLoggerInstance() {
    //switch (logging_type_) {
     // case LoggingType::ON:
        return ReorderedPhyLogLogManager::GetInstance();
      //default:
//        return DummyLogManager::GetInstance();
//    }
  }

  static CheckpointManager &GetCheckpointerInstance() {
    switch (checkpoint_type_) {
      case CHECKPOINT_TYPE_PHYLOG:
        return PhyLogCheckpointManager::GetInstance();
      default:
      return DummyCheckpointManager::GetInstance();
    }
  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type, TimerType timer_type, bool detailed_csv = false) {
    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;
    timer_type_ = timer_type;
    generate_detailed_csv_ = detailed_csv;
  }

  inline static LoggingType GetLoggingType() { return logging_type_; }

  inline static CheckpointType GetCheckpointType() { return checkpoint_type_; }

  inline static TimerType GetTimerType() { return timer_type_; }

  inline static bool GenerateDetailedCsv() { return generate_detailed_csv_; }

  /* Statistics */
  //static void StartTxnTimer(size_t eid, WorkerContext *worker_ctx);
  //static void StopTimersByPepoch(size_t persist_eid, WorkerContext *worker_ctx);
  
  static uint64_t GetCurrentTimeInUsec() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
  }

private:
  static LoggingType logging_type_;
  static CheckpointType checkpoint_type_;
  static TimerType  timer_type_;
  static bool generate_detailed_csv_;

};
} // namespace gc
} // namespace peloton
