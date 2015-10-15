/*-------------------------------------------------------------------------
 *
 * ariesbackendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesbackendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "aries_backend_logger.h"

#include <iostream>
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/frontend_logger.h"

namespace peloton {
namespace logging {

AriesBackendLogger* AriesBackendLogger::GetInstance(){
  thread_local static AriesBackendLogger aries_backend_logger;
  return &aries_backend_logger;
}

/**
 * @brief log LogRecord
 * @param log record 
 */
void AriesBackendLogger::Log(LogRecord* record){
  // Enqueue the serialized log record into the queue
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
  if(record->GetType() == LOGRECORD_TYPE_TRANSACTION_END)  {
    auto& log_manager = logging::LogManager::GetInstance();
    FrontendLogger *frontend = log_manager.GetFrontendLogger(logging_type);
    frontend->NotifyFrontend(true);
  }
}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t AriesBackendLogger::GetLocalQueueSize(void) const{
  return local_queue.size();
}

LogRecord* AriesBackendLogger::GetTupleRecord(LogRecordType log_record_type,
                                              txn_id_t txn_id,
                                              oid_t table_oid,
                                              ItemPointer insert_location,
                                              ItemPointer delete_location,
                                              void* data,
                                              oid_t db_oid){

  // Build the log record
  switch(log_record_type){
    case LOGRECORD_TYPE_TUPLE_INSERT:  {
      log_record_type = LOGRECORD_TYPE_ARIES_TUPLE_INSERT;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_DELETE:  {
      log_record_type = LOGRECORD_TYPE_ARIES_TUPLE_DELETE;
      break;
    }

    case LOGRECORD_TYPE_TUPLE_UPDATE:  {
      log_record_type = LOGRECORD_TYPE_ARIES_TUPLE_UPDATE;
      break;
    }

    default:  {
      assert(false);
      break;
    }
  }

  LogRecord *record = new TupleRecord(log_record_type,
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
