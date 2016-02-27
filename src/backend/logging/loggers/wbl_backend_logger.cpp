/*-------------------------------------------------------------------------
 *
 * wbl_backend_logger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wbl_backend_logger.cpp
 *
 *-------------------------------------------------------------------------
 */


#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wbl_backend_logger.h"

namespace peloton {
namespace logging {

WriteBehindBackendLogger *WriteBehindBackendLogger::GetInstance() {
  thread_local static WriteBehindBackendLogger instance;
  return &instance;
}

/**
 * @brief log LogRecord
 * @param log record
 */
void WriteBehindBackendLogger::Log(LogRecord *record) {
  // Enqueue the serialized log record into the queue
  record->Serialize(output_buffer);

  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(record);
  }
}

LogRecord *WriteBehindBackendLogger::GetTupleRecord(LogRecordType log_record_type,
                                                txn_id_t txn_id,
                                                oid_t table_oid,
                                                ItemPointer insert_location,
                                                ItemPointer delete_location,
                                                void *data, oid_t db_oid) {
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
      assert(false);
      break;
    }
  }

  // Don't make use of "data" in case of peloton log records
  data = nullptr;

  // Build the tuple log record
  LogRecord *tuple_record =
      new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                      delete_location, data, db_oid);

  return tuple_record;
}

}  // namespace logging
}  // namespace peloton
