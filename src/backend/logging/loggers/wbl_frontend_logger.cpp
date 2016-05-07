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

#define POSSIBLY_DIRTY_GRANT_SIZE 10000000; // ten million seems reasonable

namespace peloton {
namespace logging {

struct WriteBehindLoggerRecord{
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
	struct WriteBehindLoggerRecord record;
	record.persistent_commit_id = max_collected_commit_id;
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	cid_t new_grant = txn_manager.GetCurrentCommitId() + POSSIBLY_DIRTY_GRANT_SIZE;
	// get current highest dispense commit id
	record.max_possible_dirty_commit_id = new_grant;
	if (!fwrite(&record, sizeof(WriteBehindLoggerRecord), 1, log_file)){
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

	//set new grant in txn_manager
	txn_manager.SetMaxGrantCid(new_grant);
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void WriteBehindFrontendLogger::DoRecovery() {

}

std::string WriteBehindFrontendLogger::GetLogFileName(void) {
  auto &log_manager = logging::LogManager::GetInstance();
  return log_manager.GetLogFileName();
}

void WriteBehindFrontendLogger::SetLoggerID(__attribute__((unused)) int id) {
  // do nothing
}

}  // namespace logging
}  // namespace peloton
