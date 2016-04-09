/*-------------------------------------------------------------------------
 *
 * wal_backend_logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wal_backend_logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Write Ahead Backend Logger
//===--------------------------------------------------------------------===//

class WriteAheadBackendLogger : public BackendLogger {
 public:
  WriteAheadBackendLogger(const WriteAheadBackendLogger &) = delete;
  WriteAheadBackendLogger &operator=(const WriteAheadBackendLogger &) = delete;
  WriteAheadBackendLogger(WriteAheadBackendLogger &&) = delete;
  WriteAheadBackendLogger &operator=(WriteAheadBackendLogger &&) = delete;

  WriteAheadBackendLogger() { logging_type = LOGGING_TYPE_DRAM_NVM; }

  void Log(LogRecord *record);

  void TruncateLocalQueue(oid_t offset);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid, oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location,
                            const void *data = nullptr);

 private:

  CopySerializeOutput output_buffer;
};

}  // namespace logging
}  // namespace peloton
