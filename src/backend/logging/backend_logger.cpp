//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_logger.cpp
//
// Identification: src/backend/logging/backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"
#include "backend/logging/log_record.h"
#include "backend/logging/log_manager.h"

namespace peloton {
namespace logging {

// create a backend logger
BackendLogger::BackendLogger()
    : log_buffer_(std::unique_ptr<LogBuffer>(nullptr)),
      available_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())),
      persist_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())) {
  logger_type = LOGGER_TYPE_BACKEND;
  backend_pool.reset(new VarlenPool(BACKEND_TYPE_MM));
  frontend_logger_id = -1;
}

// destroy and cleanup
BackendLogger::~BackendLogger() {
  auto &log_manager = LogManager::GetInstance();
  if (!shutdown && frontend_logger_id != -1) {
    log_manager.GetFrontendLoggersList()[frontend_logger_id]
        .get()
        ->RemoveBackendLogger(this);
  }
}

/**
 * @brief create and eturn the backend logger based on logging type
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *BackendLogger::GetBackendLogger(LoggingType logging_type) {
  BackendLogger *backend_logger = nullptr;

  if (IsBasedOnWriteAheadLogging(logging_type) == true) {
    backend_logger = new WriteAheadBackendLogger();
  } else if (IsBasedOnWriteBehindLogging(logging_type) == true) {
    backend_logger = new WriteBehindBackendLogger();
  } else {
    LOG_ERROR("Unsupported logging type");
  }

  return backend_logger;
}

/**
 * @brief log log a log record
 * @param log record
 */
void BackendLogger::Log(LogRecord *record) {
  // Enqueue the serialized log record into the queue
  record->Serialize(output_buffer);

  this->log_buffer_lock.Lock();
  if (!log_buffer_) {
    LOG_TRACE("Acquire the first log buffer in backend logger");
    this->log_buffer_lock.Unlock();
    std::unique_ptr<LogBuffer> new_buff =
        std::move(available_buffer_pool_->Get());
    this->log_buffer_lock.Lock();
    log_buffer_ = std::move(new_buff);
  }
  // update max logged commit id
  if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
    auto new_log_commit_id = record->GetTransactionId();
    PL_ASSERT(new_log_commit_id > highest_logged_commit_message);
    highest_logged_commit_message = new_log_commit_id;
    logging_cid_lower_bound = INVALID_CID;
  }

  // update the max logged id for the current buffer
  cid_t cur_log_id = record->GetTransactionId();

  // set if this is the max log_id seen so far
  if (cur_log_id > max_log_id_buffer) {
    log_buffer_->SetMaxLogId(cur_log_id);
    max_log_id_buffer = cur_log_id;
  }

  if (!log_buffer_->WriteRecord(record)) {
    LOG_TRACE("Log buffer is full - Attempt to acquire a new one");
    // put back a buffer
    max_log_id_buffer = 0;  // reset
    persist_buffer_pool_->Put(std::move(log_buffer_));
    this->log_buffer_lock.Unlock();

    // get a new one
    std::unique_ptr<LogBuffer> new_buff =
        std::move(available_buffer_pool_->Get());
    this->log_buffer_lock.Lock();
    log_buffer_ = std::move(new_buff);

    // write to the new log buffer
    auto success = log_buffer_->WriteRecord(record);
    if (!success) {
      LOG_ERROR("Write record to log buffer failed");
      this->log_buffer_lock.Unlock();
      return;
    }
  }

  this->log_buffer_lock.Unlock();
}

// used by the frontend logger to collect data on the current state of the backend
// returns a pair of commit ids, the first is the lower bound for values this logger may
// commit, The second is the maximum id this worker has committed
std::pair<cid_t, cid_t> BackendLogger::PrepareLogBuffers() {
  this->log_buffer_lock.Lock();
  std::pair<cid_t, cid_t> ret(INVALID_CID, INVALID_CID);
  // prepare the cid's seen so far
  if (logging_cid_lower_bound != INVALID_CID ||
      (log_buffer_ && log_buffer_->GetSize() > 0)) {
    ret.second = highest_logged_commit_message;
    if (logging_cid_lower_bound > highest_logged_commit_message) {
      ret.first = logging_cid_lower_bound;
    }
  }
  if (log_buffer_ && log_buffer_->GetSize() > 0) {
    // put back a buffer
    LOG_TRACE(
        "Move the current log buffer to buffer pool, "
        "highest_logged_commit_message: %d, logging_cid_lower_bound: %d",
        (int)highest_logged_commit_message, (int)logging_cid_lower_bound);
    persist_buffer_pool_->Put(std::move(log_buffer_));
  }
  this->log_buffer_lock.Unlock();

  auto num_log_buffer = persist_buffer_pool_->GetSize();
  while (num_log_buffer > 0) {
    local_queue.push_back(persist_buffer_pool_->Get());
    num_log_buffer--;
  }
  return ret;
}

// this method is used by the frontend logger to give back flushed buffers to
// be used by the backend logger
void BackendLogger::GrantEmptyBuffer(std::unique_ptr<LogBuffer> empty_buffer) {
  available_buffer_pool_->Put(std::move(empty_buffer));
}

// set when the frontend logger is shutting down, prevents deadlock between frontend and backend
void BackendLogger::SetShutdown(bool val) { shutdown = val; }
}  // namespace logging
}
