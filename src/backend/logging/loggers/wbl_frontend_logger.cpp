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

#define POSSIBLY_DIRTY_GRANT_SIZE 10000000; // ten million seems reasonable

namespace peloton {
namespace logging {

struct WriteBehindLogRecord{
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
	//auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	
  //==========================================================================
  // DANGER: tmporarily change the source code here!!!
  //==========================================================================
  cid_t new_grant = 0;
  //cid_t new_grant = txn_manager.GetCurrentCommitId() + POSSIBLY_DIRTY_GRANT_SIZE;
	// get current highest dispense commit id
	record.max_possible_dirty_commit_id = new_grant;
	if (!fwrite(&record, sizeof(WriteBehindLogRecord), 1, log_file)){
		LOG_ERROR("Unable to write log record");
	}

	// for now fsync every time because the cost is relatively low
	if (fsync(log_file_fd)){
		LOG_ERROR("Unable to fsync log");
	}

	// inform backend loggers they can proceed if waiting for sync
	max_flushed_commit_id = max_collected_commit_id;
	auto &manager = LogManager::GetInstance();
	manager.FrontendLoggerFlushed();

  //==========================================================================
  // DANGER: tmporarily change the source code here!!!
  //==========================================================================
	//set new grant in txn_manager
	//txn_manager.SetMaxGrantCid(new_grant);
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
	long record_num = stat_buf.st_size/sizeof(WriteBehindLogRecord);
	if (stat_buf.st_size%sizeof(WriteBehindLogRecord)){
		if (!ftruncate(log_file_fd, record_num*sizeof(WriteBehindLogRecord))){
			LOG_ERROR("ftruncate failed on recovery");
		}
	}

	// if no records, return
	if (record_num == 0){
		return;
	}

	long first_record_offset = (record_num-1)*sizeof(WriteBehindLogRecord);

	// seek to current record;
	fseek(log_file, first_record_offset, SEEK_SET);

	// read last record to get the possibly dirty span
	WriteBehindLogRecord most_recent_log_record;
	if (!fread(&most_recent_log_record, sizeof(WriteBehindLogRecord), 1, log_file)){
		LOG_ERROR("log recovery read failed");
	}

	// set the next cid of the transaction manager
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.SetNextCid(most_recent_log_record.max_possible_dirty_commit_id+1);

  //==========================================================================
  // DANGER: tmporarily change the source code here!!!
  //==========================================================================
	// set the dirty span of the transaction manager
	// txn_manager.SetDirtyRange(std::make_pair(most_recent_log_record.persistent_commit_id,
	// 		most_recent_log_record.max_possible_dirty_commit_id));

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
