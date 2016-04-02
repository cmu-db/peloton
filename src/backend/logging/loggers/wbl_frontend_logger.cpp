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

namespace peloton {
namespace logging {

size_t GetLogFileSize(int log_file_fd);

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

bool ReadTransactionRecordHeader(TransactionRecord &txn_record, FILE *log_file,
                                 size_t log_file_size);

bool ReadTupleRecordHeader(TupleRecord &tuple_record, FILE *log_file,
                           size_t log_file_size);

/**
 * @brief create NVM backed log pool
 */
WriteBehindFrontendLogger::WriteBehindFrontendLogger() {
  logging_type = LOGGING_TYPE_NVM_NVM;

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
  // Clean up the global record pool
  global_peloton_log_record_pool.Clear();

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
  std::vector<txn_id_t> committed_txn_list;
  std::vector<txn_id_t> not_committed_txn_list;
  std::set<oid_t> modified_tile_group_set;

  //===--------------------------------------------------------------------===//
  // Collect the log records
  //===--------------------------------------------------------------------===//

  size_t global_queue_size = global_queue.size();
  for(oid_t global_queue_itr = 0;
      global_queue_itr < global_queue_size;
      global_queue_itr++) {

    if(global_queue[global_queue_itr] == nullptr) {
      continue;
    }

    switch (global_queue[global_queue_itr]->GetType()) {
      case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        global_peloton_log_record_pool.CreateTxnLogList(
            global_queue[global_queue_itr]->GetTransactionId());
        break;

      case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        committed_txn_list.push_back(
            global_queue[global_queue_itr]->GetTransactionId());
        break;

      case LOGRECORD_TYPE_TRANSACTION_ABORT:
        // Nothing to be done for abort
        break;

      case LOGRECORD_TYPE_TRANSACTION_END:
      case LOGRECORD_TYPE_TRANSACTION_DONE:
        // if a txn is not committed (aborted or active), log records will be
        // removed here
        // Note that list is not be removed immediately, it is removed only
        // after flush and commit.
        not_committed_txn_list.push_back(
            global_queue[global_queue_itr]->GetTransactionId());
        break;

      case LOGRECORD_TYPE_WBL_TUPLE_INSERT:
      case LOGRECORD_TYPE_WBL_TUPLE_DELETE:
      case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {

        LogRecord* log_record = global_queue[global_queue_itr].release();
        TupleRecord* tuple_record = reinterpret_cast<TupleRecord*>(log_record);

        // Check the commit information
        auto status =
            CollectTupleRecord(std::unique_ptr<TupleRecord>(tuple_record));

        // Add it to the set of modified tile groups
        if (status.first == true) {
          auto location = status.second.block;
          if (location != INVALID_OID) {
            modified_tile_group_set.insert(location);
          }
        }

      } break;

      case LOGRECORD_TYPE_INVALID:
      default:
        throw Exception("Invalid or unrecogized log record found");
        break;
    }

  }

  // Clean up the frontend logger's queue
  global_queue.clear();

  //===--------------------------------------------------------------------===//
  // Write out the log records
  //===--------------------------------------------------------------------===//

  // If committed txn list is not empty
  if (committed_txn_list.empty() == false) {
    //===--------------------------------------------------------------------===//
    // SYNC 1: Sync the TGs
    //===--------------------------------------------------------------------===//

    SyncTileGroups(modified_tile_group_set);

    //===--------------------------------------------------------------------===//
    // SYNC 2: Sync the log for TXN COMMIT record
    //===--------------------------------------------------------------------===//

    // Write out all the committed log records
    size_t written_log_record_count = WriteLogRecords(committed_txn_list);

    // Now, write a committing log entry to file
    // Piggyback the number of written log records as a "txn_id" in this record
    WriteTransactionLogRecord(TransactionRecord(
        LOGRECORD_TYPE_TRANSACTION_COMMIT, written_log_record_count));

    //===--------------------------------------------------------------------===//
    // SYNC 3: Sync the changes to TG headers
    //===--------------------------------------------------------------------===//

    // Toggle the commit marks
    auto tile_group_header_set = ToggleCommitMarks(committed_txn_list);

    // Sync the TG headers
    SyncTileGroupHeaders(tile_group_header_set);

    //===--------------------------------------------------------------------===//
    // SYNC 4 : Sync the log for TXN DONE record
    //===--------------------------------------------------------------------===//

    // Write out a transaction done log record to file
    WriteTransactionLogRecord(
        TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));
  }

  //===--------------------------------------------------------------------===//
  // Clean up finished transaction log lists
  //===--------------------------------------------------------------------===//

  // remove any finished txn logs
  for (txn_id_t txn_id : not_committed_txn_list) {
    global_peloton_log_record_pool.RemoveTxnLogRecordList(txn_id);
  }

  // Notify the backend loggers
  {
    for(auto backend_logger : backend_loggers){
      backend_logger->FinishedFlushing();
    }
  }

}

size_t WriteBehindFrontendLogger::WriteLogRecords(
    std::vector<txn_id_t> committed_txn_list) {
  size_t total_txn_log_records = 0;

  // Write out the log records of all the committed transactions to log file
  for (txn_id_t txn_id : committed_txn_list) {
    // Locate the transaction log list for this txn id
    auto exists_txn_log_list =
        global_peloton_log_record_pool.ExistsTxnLogRecordList(txn_id);
    if (exists_txn_log_list == false) {
      continue;
    }

    auto& txn_log_record_list = global_peloton_log_record_pool.txn_log_table[txn_id];
    size_t txn_log_record_list_size = txn_log_record_list.size();
    total_txn_log_records += txn_log_record_list_size;

    // Write out all the records in the list
    for (size_t txn_log_list_itr = 0;
        txn_log_list_itr < txn_log_record_list_size;
        txn_log_list_itr++) {

      TupleRecord *record = txn_log_record_list.at(txn_log_list_itr).get();

      // Write out the log record
      fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
             log_file);
    }
  }

  // No need to flush now, will flush later in WriteTxnLog
  return total_txn_log_records;
}

void WriteBehindFrontendLogger::WriteTransactionLogRecord(
    TransactionRecord txn_log_record) {
  txn_log_record.Serialize(output_buffer);
  fwrite(txn_log_record.GetMessage(), sizeof(char),
         txn_log_record.GetMessageLength(), log_file);

  // Then, flush
  int ret = fflush(log_file);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(log_file_fd);
  fsync_count++;
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}

std::set<storage::TileGroupHeader *>
WriteBehindFrontendLogger::ToggleCommitMarks(
    std::vector<txn_id_t> committed_txn_list) {
  // Headers modified
  std::set<storage::TileGroupHeader *> tile_group_headers;

  // Toggle commit marks
  for (txn_id_t txn_id : committed_txn_list) {
    auto exists_txn_log_list =
        global_peloton_log_record_pool.ExistsTxnLogRecordList(txn_id);
    if (exists_txn_log_list == false) {
      continue;
    }

    auto& txn_log_record_list = global_peloton_log_record_pool.txn_log_table[txn_id];
    size_t txn_log_record_list_size = txn_log_record_list.size();

    for (size_t txn_log_list_itr = 0;
        txn_log_list_itr < txn_log_record_list_size;
        txn_log_list_itr++) {

      // Get the log record
      TupleRecord *record = txn_log_record_list.at(txn_log_list_itr).get();
      cid_t current_commit_id = INVALID_CID;

      auto record_type = record->GetType();
      switch (record_type) {
        case LOGRECORD_TYPE_WBL_TUPLE_INSERT: {
          // Set insert commit mark
          auto insert_location = record->GetInsertLocation();
          auto info = SetInsertCommitMark(insert_location);
          current_commit_id = info.first;
          tile_group_headers.insert(info.second);
        } break;

        case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
          // Set delete commit mark
          auto delete_location = record->GetDeleteLocation();
          auto info = SetDeleteCommitMark(delete_location);
          current_commit_id = info.first;
          tile_group_headers.insert(info.second);
        } break;

        case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
          // Set delete commit mark
          auto delete_location = record->GetDeleteLocation();
          auto info = SetDeleteCommitMark(delete_location);
          current_commit_id = info.first;
          tile_group_headers.insert(info.second);

          // Set insert commit mark
          auto insert_location = record->GetInsertLocation();
          info = SetInsertCommitMark(insert_location);
          current_commit_id = info.first;
          tile_group_headers.insert(info.second);
        } break;

        default:
          break;
      }

      // Update latest commit id
      if (latest_commit_id < current_commit_id) {
        latest_commit_id = current_commit_id;
      }
    }

    // TODO: All records are committed, its safe to remove them now
    global_peloton_log_record_pool.RemoveTxnLogRecordList(txn_id);
  }

  return tile_group_headers;
}

void WriteBehindFrontendLogger::SyncTileGroupHeaders(
    std::set<storage::TileGroupHeader *> tile_group_header_set) {
  // Sync all the tile group headers
  for (auto tile_group_header : tile_group_header_set) {
    tile_group_header->Sync();
  }
}

void WriteBehindFrontendLogger::SyncTileGroups(std::set<oid_t> tile_group_set) {
  auto &manager = catalog::Manager::GetInstance();

  // Sync all the tile groups
  for (auto tile_group_block : tile_group_set) {
    auto tile_group = manager.GetTileGroup(tile_group_block);
    assert(tile_group != nullptr);

    tile_group->Sync();
  }
}

std::pair<bool, ItemPointer> WriteBehindFrontendLogger::CollectTupleRecord(
    std::unique_ptr<TupleRecord> record) {
  if (record == nullptr) {
    return std::make_pair(false, INVALID_ITEMPOINTER);
  }

  auto record_type = record->GetType();
  if (record_type == LOGRECORD_TYPE_WBL_TUPLE_INSERT ||
      record_type == LOGRECORD_TYPE_WBL_TUPLE_DELETE ||
      record_type == LOGRECORD_TYPE_WBL_TUPLE_UPDATE) {
    // Collect this log record
    auto insert_location = record->GetInsertLocation();
    auto status = global_peloton_log_record_pool.AddLogRecord(std::move(record));

    if (status != 0) {
      return std::make_pair(false, INVALID_ITEMPOINTER);
    }

    // Return the insert location associated with this tuple record
    // The location is valid only for insert and update records
    return std::make_pair(true, insert_location);
  }

  return std::make_pair(false, INVALID_ITEMPOINTER);
}

std::pair<cid_t, storage::TileGroupHeader *>
WriteBehindFrontendLogger::SetInsertCommitMark(ItemPointer location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  assert(tile_group != nullptr);
  auto tile_group_header = tile_group->GetHeader();
  assert(tile_group_header != nullptr);

  // Set the commit mark
  tile_group_header->SetInsertCommit(location.offset, true);
  LOG_TRACE("<%p, %lu> : slot is insert committed", tile_group.get(),
            location.offset);

  // Update max oid
  if (max_oid < location.block) {
    max_oid = location.block;
  }

  auto begin_commit_id = tile_group_header->GetBeginCommitId(location.offset);
  return std::make_pair(begin_commit_id, tile_group_header);
}

std::pair<cid_t, storage::TileGroupHeader *>
WriteBehindFrontendLogger::SetDeleteCommitMark(ItemPointer location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  assert(tile_group != nullptr);
  auto tile_group_header = tile_group->GetHeader();
  assert(tile_group_header != nullptr);

  // Set the commit mark
  tile_group_header->SetDeleteCommit(location.offset, true);
  LOG_TRACE("<%p, %lu> : slot is delete committed", tile_group.get(),
            location.offset);

  // Update max oid
  if (max_oid < location.block) {
    max_oid = location.block;
  }

  auto end_commit_id = tile_group_header->GetEndCommitId(location.offset);
  return std::make_pair(end_commit_id, tile_group_header);
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteBehindFrontendLogger::DoRecovery() {
  // Set log file size
  log_file_size = GetLogFileSize(log_file_fd);

  // Go over the log size if needed
  if (log_file_size > 0) {
    bool reached_end_of_file = false;
    oid_t recovery_log_record_count = 0;

    // check whether first item is LOGRECORD_TYPE_TRANSACTION_COMMIT
    // if not, no need to do recovery.
    // if yes, need to replay all log records before we hit
    // LOGRECORD_TYPE_TRANSACTION_DONE
    bool need_recovery = NeedRecovery();
    LOG_TRACE("Need recovery : %d", need_recovery);

    if (need_recovery == true) {
      TransactionRecord dummy_transaction_record(LOGRECORD_TYPE_INVALID);
      cid_t current_commit_id = INVALID_CID;

      // Go over each log record in the log file
      while (reached_end_of_file == false) {
        // Read the first byte to identify log record type
        // If that is not possible, then wrap up recovery
        LogRecordType log_type = GetNextLogRecordType(log_file, log_file_size);
        recovery_log_record_count++;

        switch (log_type) {
          case LOGRECORD_TYPE_TRANSACTION_DONE:
          case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
            // read but do nothing
            ReadTransactionRecordHeader(dummy_transaction_record, log_file,
                                        log_file_size);
          } break;

          case LOGRECORD_TYPE_WBL_TUPLE_INSERT: {
            TupleRecord insert_record(LOGRECORD_TYPE_WBL_TUPLE_INSERT);
            ReadTupleRecordHeader(insert_record, log_file, log_file_size);

            auto insert_location = insert_record.GetInsertLocation();
            auto info = SetInsertCommitMark(insert_location);
            current_commit_id = info.first;
          } break;

          case LOGRECORD_TYPE_WBL_TUPLE_DELETE: {
            TupleRecord delete_record(LOGRECORD_TYPE_WBL_TUPLE_DELETE);
            ReadTupleRecordHeader(delete_record, log_file, log_file_size);

            auto delete_location = delete_record.GetDeleteLocation();
            auto info = SetDeleteCommitMark(delete_location);
            current_commit_id = info.first;
          } break;

          case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
            TupleRecord update_record(LOGRECORD_TYPE_WBL_TUPLE_UPDATE);
            ReadTupleRecordHeader(update_record, log_file, log_file_size);

            auto delete_location = update_record.GetDeleteLocation();
            SetDeleteCommitMark(delete_location);

            auto insert_location = update_record.GetInsertLocation();
            auto info = SetInsertCommitMark(insert_location);
            current_commit_id = info.first;
          } break;

          default:
            reached_end_of_file = true;
            break;
        }
      }

      // Update latest commit id
      if (latest_commit_id < current_commit_id) {
        latest_commit_id = current_commit_id;
      }

      // write out a trasaction done log record to file
      // to avoid redo next time during recovery
      WriteTransactionLogRecord(
          TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE));

      LOG_INFO("Recovery_log_record_count : %lu", recovery_log_record_count);
    }

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);
  }
}

// Check whether need to recovery, if yes, reset fseek to the right place.
bool WriteBehindFrontendLogger::NeedRecovery(void) {
  // Otherwise, read the last transaction record
  fseek(log_file, -TransactionRecord::GetTransactionRecordSize(), SEEK_END);

  // Get its type
  auto log_record_type = GetNextLogRecordType(log_file, log_file_size);

  // Check if the previous transaction run is broken
  if (log_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
    TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_COMMIT);

    // read the last written out transaction log record
    if (ReadTransactionRecordHeader(txn_record, log_file, log_file_size) ==
        false) {
      return false;
    }

    // Peloton log records items have fixed size.
    // Compute log offset based on txn_id
    size_t tuple_log_record_count = txn_record.GetTransactionId();

    size_t rollback_offset =
        tuple_log_record_count * TupleRecord::GetTupleRecordSize() +
        TransactionRecord::GetTransactionRecordSize();

    // Rollback to the computed offset
    fseek(log_file, -rollback_offset, SEEK_END);
    return true;
  } else {
    return false;
  }
}

std::string WriteBehindFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

}  // namespace logging
}  // namespace peloton
