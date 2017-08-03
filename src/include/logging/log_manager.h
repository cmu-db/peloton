//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cmath>

#include "common/macros.h"
#include "concurrency/transaction.h"
#include "logging/worker_context.h"

namespace peloton {

namespace storage {
  class TileGroupHeader;
}

namespace logging {


/* Per worker thread local context */
extern thread_local WorkerContext* tl_worker_ctx;

// loggers are created before workers.
class LogManager {
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

public:
  LogManager() : global_persist_epoch_id_(INVALID_EID) {}

  virtual ~LogManager() {}

  virtual void SetDirectories(const std::vector<std::string> &logging_dirs) = 0;

  virtual const std::vector<std::string> &GetDirectories() = 0;

  void SetRecoveryThreadCount(const size_t &recovery_thread_count) {
    recovery_thread_count_ = recovery_thread_count;
  }

  virtual void RegisterWorker(size_t thread_id) = 0;
  virtual void DeregisterWorker() = 0;

  virtual void DoRecovery(const size_t &begin_eid) = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

  virtual void StartTxn(concurrency::Transaction *txn);

  virtual void FinishPendingTxn();

  size_t GetPersistEpochId() {
    return global_persist_epoch_id_.load();
  }

protected:
  // Don't delete the returned pointer
  inline LogBuffer * RegisterNewBufferToEpoch(std::unique_ptr<LogBuffer> log_buffer_ptr) {
    LOG_TRACE("Worker %d Register buffer to epoch %d", (int) tl_worker_ctx->worker_id, (int) tl_worker_ctx->current_commit_eid);
    PL_ASSERT(log_buffer_ptr && log_buffer_ptr->Empty());
    PL_ASSERT(tl_worker_ctx);
    size_t eid_idx = tl_worker_ctx->current_commit_eid % concurrency::EpochManager::GetEpochQueueCapacity();
    tl_worker_ctx->per_epoch_buffer_ptrs[eid_idx].push(std::move(log_buffer_ptr));
    return tl_worker_ctx->per_epoch_buffer_ptrs[eid_idx].top().get();
  }


  inline size_t HashToLogger(oid_t worker_id) {
    return ((size_t) worker_id) % logger_count_;
  }


protected:
  size_t logger_count_;

  size_t recovery_thread_count_ = 1;

  std::atomic<size_t> global_persist_epoch_id_;

};

}
}
