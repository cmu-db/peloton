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

#pragma once

#include "backend/common/types.h"
#include "backend/logging/backend_logger.h"
#include "backend/logging/log_buffer.h"
#include "backend/logging/circular_buffer_pool.h"

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

  void Log(LogRecord *record);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid, oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location, void *data = nullptr);

  void PrepareLogBuffers();

  void GrantEmptyBuffer(std::unique_ptr<LogBuffer>);

 private:
  // temporary serialization buffer
  CopySerializeOutput output_buffer;

  // the current buffer
  std::unique_ptr<LogBuffer> log_buffer_;

  // the pool of available buffers
  std::unique_ptr<BufferPool> available_buffer_pool_;

  // the pool of buffers to persist
  std::unique_ptr<BufferPool> persist_buffer_pool_;
};

}  // namespace logging
}  // namespace peloton
