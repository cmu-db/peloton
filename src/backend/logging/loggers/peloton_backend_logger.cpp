/*-------------------------------------------------------------------------
 *
 * pelotonbackendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonbackendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <iostream>

#include "backend/logging/records/tuple_record.h"
#include "backend/logging/loggers/peloton_backend_logger.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"

namespace peloton {
namespace logging {

PelotonBackendLogger* PelotonBackendLogger::GetInstance(){
  thread_local static PelotonBackendLogger pInstance;
  return &pInstance;
}

/**
 * @brief log LogRecord
 * @param log record 
 */
void PelotonBackendLogger::Log(LogRecord* record){
  // Enqueue the serialized log record into the queue
  record->Serialize(output_buffer);
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    local_queue.push_back(record);
  }
  if(record->GetType() == LOGRECORD_TYPE_TRANSACTION_END)  {
    auto& log_manager = logging::LogManager::GetInstance();
    log_manager.NotifyFrontendLogger(logging_type, true);
  }
}

LogRecord* PelotonBackendLogger::GetTupleRecord(LogRecordType log_record_type, 
                                              txn_id_t txn_id, 
                                              oid_t table_oid, 
                                              ItemPointer insert_location, 
                                              ItemPointer delete_location, 
                                              void* data,
                                              oid_t db_oid){
  // Just, ignore the data
  data = nullptr;

  // Build the log record
  switch(log_record_type){
    case LOGRECORD_TYPE_TUPLE_INSERT:  {
      log_record_type = LOGRECORD_TYPE_PELOTON_TUPLE_INSERT; 
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE:  {
      log_record_type = LOGRECORD_TYPE_PELOTON_TUPLE_DELETE; 
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE:  {
      log_record_type = LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE; 
      break;
    }

    default:  {
      assert(false);
      break;
    }
  }

  LogRecord* record = new TupleRecord(log_record_type, 
                                      txn_id,
                                      table_oid,
                                      insert_location,
                                      delete_location,
                                      data,
                                      db_oid);
  return record;
}

}  // namespace logging
}  // namespace peloton
