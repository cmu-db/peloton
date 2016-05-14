//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_backend_logger.cpp
//
// Identification: src/backend/logging/loggers/wbl_backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"

namespace peloton {
namespace logging {

void WriteBehindBackendLogger::Log(LogRecord *record) {
  // if we are committing, sync all data before taking the lock
  if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT){
	  SyncDataForCommit();
  }
  log_buffer_lock.Lock();
  switch (record->GetType()) {
    case LOGRECORD_TYPE_TRANSACTION_COMMIT:
      highest_logged_commit_message = record->GetTransactionId();
    // fallthrough
    case LOGRECORD_TYPE_TRANSACTION_ABORT:
    case LOGRECORD_TYPE_TRANSACTION_BEGIN:
    case LOGRECORD_TYPE_TRANSACTION_DONE:
    case LOGRECORD_TYPE_TRANSACTION_END: {
      if(logging_cid_lower_bound < record->GetTransactionId()-1 ){
    	  logging_cid_lower_bound = record->GetTransactionId()-1;
      }
      break;
    }
    case LOGRECORD_TYPE_WBL_TUPLE_DELETE:{
        tile_groups_to_sync_.insert(((TupleRecord *)record)->GetDeleteLocation().block);
        break;
      }
    case LOGRECORD_TYPE_WBL_TUPLE_INSERT:{
    	tile_groups_to_sync_.insert(((TupleRecord *)record)->GetInsertLocation().block);
        break;
      }
    case LOGRECORD_TYPE_WBL_TUPLE_UPDATE: {
    	tile_groups_to_sync_.insert(((TupleRecord *)record)->GetDeleteLocation().block);
    	tile_groups_to_sync_.insert(((TupleRecord *)record)->GetInsertLocation().block);
      break;
    }
    default:
      LOG_INFO("Invalid log record type");
      break;
  }

  log_buffer_lock.Unlock();
}

void WriteBehindBackendLogger::SyncDataForCommit(){
	auto &manager = catalog::Manager::GetInstance();

  // Sync the tiles in the modified tile groups and their headers
	for (oid_t tile_group_id : tile_groups_to_sync_){
		auto tile_group = manager.GetTileGroup(tile_group_id);
		tile_group->Sync();
		tile_group->GetHeader()->Sync();
	}

	// Clear the list of tile groups
	tile_groups_to_sync_.clear();
}

LogRecord *WriteBehindBackendLogger::GetTupleRecord(
    LogRecordType log_record_type, txn_id_t txn_id, oid_t table_oid,
    oid_t db_oid, ItemPointer insert_location, ItemPointer delete_location,
    __attribute__((unused)) const void *data) {
  // Figure the log record type
  switch (log_record_type) {
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_INSERT;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_DELETE;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      log_record_type = LOGRECORD_TYPE_WBL_TUPLE_UPDATE;
      break;
    }

    default: {
      ALWAYS_ASSERT(false);
      break;
    }
  }

  // Don't make use of "data" in case of peloton log records
  // Build the tuple log record
  LogRecord *tuple_record =
      new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                      delete_location, nullptr, db_oid);

  return tuple_record;
}

}  // namespace logging
}  // namespace peloton
