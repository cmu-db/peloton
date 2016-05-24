//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_frontend_logger.cpp
//
// Identification: src/backend/logging/loggers/wbl_frontend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/common/exception.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/logging/loggers/wbl_frontend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"
#include "backend/logging/logging_util.h"
#include "backend/logging/log_manager.h"

#define POSSIBLY_DIRTY_GRANT_SIZE 10000000;  // ten million seems reasonable

namespace peloton {
namespace logging {

struct WriteBehindLogRecord {
  cid_t persistent_commit_id;
  cid_t max_possible_dirty_commit_id;
};

// TODO for now, these helper routines are defined here, and also use
// some routines from the LoggingUtil class. Make sure that all places where
// these helper routines are called in this file use the LoggingUtil class
size_t GetLogFileSize(int fd) {
  struct stat log_stats;
  fstat(fd, &log_stats);
  return log_stats.st_size;
}

/**
 * @brief create NVM backed log pool
 */
WriteBehindFrontendLogger::WriteBehindFrontendLogger() {
  logging_type = LOGGING_TYPE_NVM_WBL;

  // open log file and file descriptor
  // we open it in append + binary mode
  log_file = fopen(GetLogFileName().c_str(), "ab+");
  if (log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }
}

/**
 * @brief clean NVM space
 */
WriteBehindFrontendLogger::~WriteBehindFrontendLogger() {
  // Clean up the frontend logger's queue
  global_queue.clear();
}

//===--------------------------------------------------------------------===//
// Active Processing
//===--------------------------------------------------------------------===//

/**
 * @brief flush all log records to the file
 */
void WriteBehindFrontendLogger::FlushLogRecords(void) {
  struct WriteBehindLogRecord record;
  record.persistent_commit_id = max_collected_commit_id;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  cid_t new_grant =
      txn_manager.GetCurrentCommitId() + POSSIBLY_DIRTY_GRANT_SIZE;
  // get current highest dispense commit id
  record.max_possible_dirty_commit_id = new_grant;
  if (!fwrite(&record, sizeof(WriteBehindLogRecord), 1, log_file)) {
    LOG_ERROR("Unable to write log record");
  }

  long curr_seq_num = 0;
  size_t global_queue_size = global_queue.size();
  if (replicating_ && global_queue_size > 0) {
    size_t global_queue_size = global_queue.size();

    size_t write_size = 0;
    size_t rep_array_offset = 0;
    std::unique_ptr<char> replication_array = nullptr;
    TransactionRecord delimiter_rec(LOGRECORD_TYPE_ITERATION_DELIMITER,
                                    this->max_collected_commit_id);
    delimiter_rec.Serialize(output_buffer);

    // find the size we need to write out
    for (oid_t global_queue_itr = 0; global_queue_itr < global_queue_size;
         global_queue_itr++) {
      write_size += global_queue[global_queue_itr]->GetSize();
    }
    if (max_collected_commit_id != max_flushed_commit_id) {
      write_size += delimiter_rec.GetMessageLength();
    }
    replication_array.reset(new char[write_size]);

    for (oid_t global_queue_itr = 0; global_queue_itr < global_queue_size;
         global_queue_itr++) {
      auto &log_buffer = global_queue[global_queue_itr];

      memcpy(replication_array.get() + rep_array_offset, log_buffer->GetData(),
             log_buffer->GetSize());
      rep_array_offset += log_buffer->GetSize();
    }

    if (max_collected_commit_id != max_flushed_commit_id) {
      PL_ASSERT(rep_array_offset + delimiter_rec.GetMessageLength() ==
                write_size);
      memcpy(replication_array.get() + rep_array_offset,
             delimiter_rec.GetMessage(), delimiter_rec.GetMessageLength());
      // send the request
      rep_array_offset += delimiter_rec.GetMessageLength();
    }

    networking::LogRecordReplayRequest request;
    request.set_log(replication_array.get(), write_size);
    curr_seq_num = replication_seq_++;
    request.set_sync_type(replication_mode_);
    request.set_sequence_number(curr_seq_num);
    networking::LogRecordReplayResponse response;
    replication_stub_->LogRecordReplay(controller_.get(), &request, &response,
                                       nullptr);

    global_queue.clear();
  }

  // for now fsync every time because the cost is relatively low
  if (fsync(log_file_fd)) {
    LOG_ERROR("Unable to fsync log");
  }

  // if replicating in the proper mode, wait here
  if (curr_seq_num > 0 && (replication_mode_ == networking::SYNC ||
                           replication_mode_ == networking::SEMISYNC)) {
    // wait for the response with the proper sequence number
    while (remote_done_.load() < curr_seq_num)
      ;
  }

  // inform backend loggers they can proceed if waiting for sync
  max_flushed_commit_id = max_collected_commit_id;
  auto &manager = LogManager::GetInstance();
  manager.FrontendLoggerFlushed();

  // set new grant in txn_manager
  txn_manager.SetMaxGrantCid(new_grant);
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteBehindFrontendLogger::DoRecovery() {
  // read file, truncate any trailing log data
  // get file size

  struct stat stat_buf;
  fstat(log_file_fd, &stat_buf);

  // calculate number of records
  long record_num = stat_buf.st_size / sizeof(WriteBehindLogRecord);
  if (stat_buf.st_size % sizeof(WriteBehindLogRecord)) {
    if (!ftruncate(log_file_fd, record_num * sizeof(WriteBehindLogRecord))) {
      LOG_ERROR("ftruncate failed on recovery");
    }
  }

  // if no records, return
  if (record_num == 0) {
    return;
  }

  long first_record_offset = (record_num - 1) * sizeof(WriteBehindLogRecord);

  // seek to current record;
  fseek(log_file, first_record_offset, SEEK_SET);

  // read last record to get the possibly dirty span
  WriteBehindLogRecord most_recent_log_record;
  if (!fread(&most_recent_log_record, sizeof(WriteBehindLogRecord), 1,
             log_file)) {
    LOG_ERROR("log recovery read failed");
  }

  // set the next cid of the transaction manager
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.SetNextCid(most_recent_log_record.max_possible_dirty_commit_id +
                         1);

  // set the dirty span of the transaction manager
  txn_manager.SetDirtyRange(
      std::make_pair(most_recent_log_record.persistent_commit_id,
                     most_recent_log_record.max_possible_dirty_commit_id));

  // for now assume that the maximum tile group oid and table tile group
  // membership info are already set
}

std::string WriteBehindFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

void WriteBehindFrontendLogger::SetLoggerID(UNUSED_ATTRIBUTE int id) {
  // do nothing
}

}  // namespace logging
}  // namespace peloton
