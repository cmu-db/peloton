//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.h
//
// Identification: src/backend/logging/loggers/wal_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.h
//
// Identification: src/backend/logging/loggers/wal_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_backend_logger.h
//
// Identification: src/backend/logging/loggers/wal_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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

  static WriteAheadBackendLogger *GetInstance(void);

  void Log(LogRecord *record);

  void TruncateLocalQueue(oid_t offset);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid, ItemPointer insert_location,
                            ItemPointer delete_location, void *data = nullptr,
                            oid_t db_oid = INVALID_OID);

 private:
  WriteAheadBackendLogger() { logging_type = LOGGING_TYPE_DRAM_NVM; }

  CopySerializeOutput output_buffer;
};

}  // namespace logging
}  // namespace peloton
