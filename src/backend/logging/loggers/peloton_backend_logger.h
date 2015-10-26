/*-------------------------------------------------------------------------
 *
 * pelotonbackendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonbackendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Peloton Backend Logger 
//===--------------------------------------------------------------------===//

class PelotonBackendLogger : public BackendLogger{

public:
  PelotonBackendLogger(const PelotonBackendLogger &) = delete;
  PelotonBackendLogger &operator=(const PelotonBackendLogger &) = delete;
  PelotonBackendLogger(PelotonBackendLogger &&) = delete;
  PelotonBackendLogger &operator=(PelotonBackendLogger &&) = delete;

  static PelotonBackendLogger* GetInstance(void);

  void Log(LogRecord* record);

  size_t GetLocalQueueSize(void) ;

  void TruncateLocalQueue(oid_t offset);

  LogRecord* GetTupleRecord(LogRecordType log_record_type, 
                            txn_id_t txn_id, 
                            oid_t table_oid, 
                            ItemPointer insert_location, 
                            ItemPointer delete_location, 
                            void* data = nullptr,
                            oid_t db_oid = INVALID_OID);

private:
    PelotonBackendLogger(){ logging_type = LOGGING_TYPE_PELOTON;}
    // XXX this flag is used for testing performance difference between
    // group flushing and fine grained flushing
    bool flush_enabled = false;
};

}  // namespace logging
}  // namespace peloton
