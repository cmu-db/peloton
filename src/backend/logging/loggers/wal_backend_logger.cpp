//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.cpp
//
// Identification: src/backend/logging/loggers/wal_backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"

namespace peloton {
namespace logging {

/**
 * @brief log LogRecord
 * @param log record
 */
void WriteAheadBackendLogger::Log(LogRecord *record) {
  // Enqueue the serialized log record into the queue
  record->Serialize(output_buffer);

  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(std::unique_ptr<LogRecord>(record));
  }
}

LogRecord *WriteAheadBackendLogger::GetTupleRecord(LogRecordType log_record_type,
                                                   txn_id_t txn_id,
                                                   oid_t table_oid, oid_t db_oid,
                                                   ItemPointer insert_location,
                                                   ItemPointer delete_location,
                                                   const void *data) {
  // Build the log record
  switch (log_record_type) {
    case LOGRECORD_TYPE_TUPLE_INSERT: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_INSERT;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_DELETE;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE: {
      log_record_type = LOGRECORD_TYPE_WAL_TUPLE_UPDATE;
      break;
    }

    default: {
      assert(false);
      break;
    }
  }

  LogRecord *record =
      new TupleRecord(log_record_type, txn_id, table_oid, insert_location,
                      delete_location, data, db_oid);

  return record;
}

}  // namespace logging
}  // namespace peloton
