//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_backend_logger.h
//
// Identification: src/backend/logging/loggers/wbl_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"
#include "backend/logging/backend_logger.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// WBL Backend Logger
//===--------------------------------------------------------------------===//

class WriteBehindBackendLogger : public BackendLogger {
 public:
  WriteBehindBackendLogger(const WriteBehindBackendLogger &) = delete;
  WriteBehindBackendLogger &operator=(const WriteBehindBackendLogger &) =
      delete;
  WriteBehindBackendLogger(WriteBehindBackendLogger &&) = delete;
  WriteBehindBackendLogger &operator=(WriteBehindBackendLogger &&) = delete;

  WriteBehindBackendLogger() { logging_type = LOGGING_TYPE_NVM_NVM; }

  //FIXME temporarily defined for wbl_backend_logger to compile code
   std::vector<std::unique_ptr<LogBuffer>> &CollectLogBuffers() {
	  return log_buffers_to_collect;
  }

  // Grant an empty buffer to use
  void GrantEmptyBuffer(__attribute__((unused)) std::unique_ptr<LogBuffer>) {
 	  //FIXME temporarily defined for wbl_backend_logger to compile code
  }

  void Log(LogRecord *record);

  void TruncateLocalQueue(oid_t offset);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid,
                            oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location, void *data = nullptr);

 private:

  CopySerializeOutput output_buffer;
};

}  // namespace logging
}  // namespace peloton
