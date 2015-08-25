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

#include "backend/logging/loggers/pelotonbackendlogger.h"
#include "backend/logging/records/tuplerecord.h"

#include <iostream>

namespace peloton {
namespace logging {

PelotonBackendLogger* PelotonBackendLogger::GetInstance(){
  static  thread_local PelotonBackendLogger pInstance; 
  return &pInstance;
}

/**
 * @brief Insert LogRecord
 * @param log record 
 */
void PelotonBackendLogger::Insert(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void PelotonBackendLogger::Delete(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

/**
 * @brief Delete LogRecord
 * @param log record 
 */
void PelotonBackendLogger::Update(LogRecord* record){
  {
    std::lock_guard<std::mutex> lock(local_queue_mutex);
    record->Serialize();
    local_queue.push_back(record);
  }
}

LogRecord* PelotonBackendLogger::GetTupleRecord(LogRecordType log_record_type, 
                                              txn_id_t txn_id, 
                                              oid_t table_oid, 
                                              ItemPointer location, 
                                              void* data,
                                              oid_t db_oid){
  // data should be nullptr here
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
                                      location,
                                      data,
                                      db_oid);
  return record;
}

}  // namespace logging
}  // namespace peloton
