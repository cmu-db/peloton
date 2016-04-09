/*-------------------------------------------------------------------------
 *
 * wbl_backend_logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wbl_backend_logger.h
 *
 *-------------------------------------------------------------------------
 */

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
  WriteBehindBackendLogger &operator=(const WriteBehindBackendLogger &) = delete;
  WriteBehindBackendLogger(WriteBehindBackendLogger &&) = delete;
  WriteBehindBackendLogger &operator=(WriteBehindBackendLogger &&) = delete;

  WriteBehindBackendLogger() { logging_type = LOGGING_TYPE_NVM_NVM; }

  void Log(LogRecord *record);

  void TruncateLocalQueue(oid_t offset);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid,
                            oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location,
                            const void *data = nullptr);

 private:

  CopySerializeOutput output_buffer;
};

}  // namespace logging
}  // namespace peloton
