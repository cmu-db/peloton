//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_logger.h
//
// Identification: src/logging/logical_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <unordered_map>

#include "concurrency/transaction_context.h"
#include "concurrency/epoch_manager.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/log_buffer_pool.h"
#include "logging/log_manager.h"
#include "logging/worker_context.h"
#include "type/types.h"
#include "common/serializer.h"
#include "common/lockfree_queue.h"
#include "common/logger.h"
#include "common/pool.h"
#include "common/synchronization/spin_lock.h"

namespace peloton {
  
namespace storage {
  class TileGroupHeader;
}

namespace logging {

  class PhyLogLogger {

  public:
    PhyLogLogger(const size_t &logger_id, const std::string &log_dir) :
      logger_id_(logger_id),
      log_dir_(log_dir),
      logger_thread_(nullptr),
      is_running_(false),
      logger_output_buffer_(), 
      persist_epoch_id_(INVALID_EID),
      worker_map_lock_(), 
      worker_map_() {}

    ~PhyLogLogger() {}

    void StartRecovery(const size_t checkpoint_eid, const size_t persist_eid, const size_t recovery_thread_count);
    void StartIndexRebulding(const size_t logger_count);

    void WaitForRecovery();
    void WaitForIndexRebuilding();

    void StartLogging() {
      is_running_ = true;
      logger_thread_.reset(new std::thread(&PhyLogLogger::Run, this));
    }

    void StopLogging() {
      is_running_ = false;
      logger_thread_->join();
    }

    void RegisterWorker(WorkerContext *phylog_worker_ctx);
    void DeregisterWorker(WorkerContext *phylog_worker_ctx);

    size_t GetPersistEpochId() const {
      return persist_epoch_id_;
    }


private:
  void Run();

  void PersistEpochBegin(FileHandle &file_handle, const size_t epoch_id);
  void PersistEpochEnd(FileHandle &file_handle, const size_t epoch_id);
  void PersistLogBuffer(FileHandle &file_handle, std::unique_ptr<LogBuffer> log_buffer);

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  void GetSortedLogFileIdList(const size_t checkpoint_eid, const size_t persist_eid);
  
  void RunRecoveryThread(const size_t thread_id, const size_t checkpoint_eid, const size_t persist_eid);

  void RunSecIndexRebuildThread(const size_t logger_count);

  void RebuildSecIndexForTable(const size_t logger_count, storage::DataTable *table);

  bool ReplayLogFile(const size_t thread_id, FileHandle &file_handle, size_t checkpoint_eid, size_t pepoch_eid);
  bool InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid);

  // Return value is the swapped txn id, either INVALID_TXNID or INITIAL_TXNID
  txn_id_t LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset);
  void UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id);

  private:
    size_t logger_id_;
    std::string log_dir_;

    // recovery threads
    std::vector<std::unique_ptr<std::thread>> recovery_threads_;
    std::vector<size_t> file_eids_;
    std::atomic<int> max_replay_file_id_;

    /* Recovery */
    // TODO: Check if we can discard the recovery pool after the recovery is done.
    // Since every thing is copied to the tile group and tile group related pool
    std::vector<std::unique_ptr<VarlenPool>> recovery_pools_;
    
    // logger thread
    std::unique_ptr<std::thread> logger_thread_;
    volatile bool is_running_;

    /* File system related */
    CopySerializeOutput logger_output_buffer_;

    /* Log buffers */
    size_t persist_epoch_id_;

    // The spin lock to protect the worker map.
    // We only update this map when creating/terminating a new worker
    common::synchronization::SpinLatch worker_map_lock_;

    // map from worker id to the worker's context.
    std::unordered_map<oid_t, std::shared_ptr<WorkerContext>> worker_map_;
  
    const std::string logging_filename_prefix_ = "log";

    const size_t sleep_period_us_ = 40000;

    const int new_file_interval_ = 500; // 500 milliseconds.
  };


}
}