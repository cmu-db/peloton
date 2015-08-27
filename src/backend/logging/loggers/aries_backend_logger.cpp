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
#include "../records/tuple_record.h"

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
}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t AriesBackendLogger::GetLocalQueueSize(void) const{
  return local_queue.size();
}

/**
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void AriesBackendLogger::TruncateLocalQueue(oid_t offset){

  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    // cleanup the queue
    local_queue.erase(local_queue.begin(),
                      local_queue.begin()+offset);

    // let's wait for the frontend logger to flush !
    // the frontend logger will call our Commit to reset it.
    wait_for_flushing = true;
  }

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
