//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.h
//
// Identification: src/logging/loggers/wal_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "logging/backend_logger.h"

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

  WriteAheadBackendLogger();

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid, oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location,
                            const void *data = nullptr);
};

}  // namespace logging
}  // namespace peloton
