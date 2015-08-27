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
  std::lock_guard<std::mutex> lock(local_queue_mutex);
  record->Serialize();
  local_queue.push_back(record);

  if(record->GetType() == LOGRECORD_TYPE_TRANSACTION_END)  {
    assert(local_queue.size()>=2);
    auto previous_record = local_queue[local_queue.size()-2];

    if(previous_record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT)  {
      log_record_count = local_queue.size();
    }else if(previous_record->GetType() == LOGRECORD_TYPE_TRANSACTION_ABORT)  {
      // Remove aborted log record
      for(oid_t log_record_itr=log_record_count; log_record_itr<local_queue.size();
          log_record_itr++){
        delete local_queue[log_record_itr];;
      }
      local_queue.erase(local_queue.begin()+log_record_count, local_queue.end());
    }
  }
}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t PelotonBackendLogger::GetLocalQueueSize(void) const{
  return log_record_count;
}

/**
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void PelotonBackendLogger::Truncate(oid_t offset){
  std::lock_guard<std::mutex> lock(local_queue_mutex);

  wait_for_flushing = true;

  local_queue.erase(local_queue.begin(), local_queue.begin()+offset);

  log_record_count -= offset;
}

LogRecord* PelotonBackendLogger::GetTupleRecord(LogRecordType log_record_type, 
                                              txn_id_t txn_id, 
                                              oid_t table_oid, 
                                              ItemPointer insert_location, 
                                              ItemPointer delete_location, 
                                              void* data,
                                              oid_t db_oid){
  data = nullptr;
  assert(data == nullptr);

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
