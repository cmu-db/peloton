/*-------------------------------------------------------------------------
 *
 * ariesbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Aries Backend Logger 
//===--------------------------------------------------------------------===//


class AriesBackendLogger : public BackendLogger{

 public:
  AriesBackendLogger(const AriesBackendLogger &) = delete;
  AriesBackendLogger &operator=(const AriesBackendLogger &) = delete;
  AriesBackendLogger(AriesBackendLogger &&) = delete;
  AriesBackendLogger &operator=(AriesBackendLogger &&) = delete;

  static AriesBackendLogger* GetInstance(void);

  void Log(LogRecord* record);

  size_t GetLocalQueueSize(void) const ;

  void TruncateLocalQueue(oid_t offset);

  LogRecord* GetTupleRecord(LogRecordType log_record_type,
                            txn_id_t txn_id,
                            oid_t table_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location,
                            void* data = nullptr,
                            oid_t db_oid = INVALID_OID);

 private:

  AriesBackendLogger(){ logging_type = LOGGING_TYPE_ARIES;}

  CopySerializeOutput output_buffer;

};

}  // namespace logging
}  // namespace peloton
