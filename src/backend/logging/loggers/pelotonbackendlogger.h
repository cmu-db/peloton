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

#include "backend/logging/backendlogger.h"

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

    void Insert(LogRecord* record);

    void Delete(LogRecord* record);

    void Update(LogRecord* record);

    LogRecord* GetTupleRecord(LogRecordType log_record_type, 
                              txn_id_t txn_id, 
                              oid_t table_oid, 
                              ItemPointer location, 
                              void* data = nullptr,
                              oid_t db_oid = INVALID_OID);

  private:

    PelotonBackendLogger(){ logging_type = LOGGING_TYPE_PELOTON;}

};

}  // namespace logging
}  // namespace peloton
