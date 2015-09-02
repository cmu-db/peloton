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

    // Get the previous record
    assert(local_queue.size() >= 2);
    auto previous_record = local_queue[local_queue.size() - 2];
    auto previous_record_type = previous_record->GetType();

    // TODO: We update local queue size only after we are sure about
    // whether the transaction is committed or aborted

    // Handle commit
    if(previous_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT)  {
      // Update the log queue offset of last committed transaction
      last_committed_txn_queue_offset = local_queue.size();
    }
    // Handle abort
    else if(previous_record_type == LOGRECORD_TYPE_TRANSACTION_ABORT)  {

      // Remove aborted log record
      for(oid_t log_record_itr=last_committed_txn_queue_offset; log_record_itr<local_queue.size();
          log_record_itr++){
        delete local_queue[log_record_itr];;
      }

      // Clean up the local queue of the backend logger
      local_queue.erase(local_queue.begin()+last_committed_txn_queue_offset, local_queue.end());
    }

  }

}

/**
 * @brief Get the local queue size
 * @return local queue size
 */
size_t PelotonBackendLogger::GetLocalQueueSize(void) const{
  return last_committed_txn_queue_offset;
}

/**
 * @brief set the wait flush to true and truncate local_queue with commit_offset
 * @param offset
 */
void PelotonBackendLogger::TruncateLocalQueue(oid_t offset){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);

    // let's wait for the frontend logger to flush !
    // the frontend logger will call our Commit to reset it.
    wait_for_flushing = true;

    // cleanup the queue
    local_queue.erase(local_queue.begin(),
                      local_queue.begin()+offset);

    // last committed log record position
    last_committed_txn_queue_offset -= offset;
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
