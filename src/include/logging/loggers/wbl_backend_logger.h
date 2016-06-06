//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_backend_logger.h
//
// Identification: src/include/logging/loggers/wbl_backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "logging/backend_logger.h"
#include "concurrency/transaction_manager_factory.h"

#include "unordered_set"

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

  WriteBehindBackendLogger() { logging_type = LOGGING_TYPE_NVM_WBL; }

  void Log(LogRecord *record);

  LogRecord *GetTupleRecord(LogRecordType log_record_type, txn_id_t txn_id,
                            oid_t table_oid, oid_t db_oid,
                            ItemPointer insert_location,
                            ItemPointer delete_location,
                            const void *data = nullptr);

  void CollectRecordsAndClear(
      std::vector<std::unique_ptr<LogRecord>> &frontend_queue);

 private:
  void SyncDataForCommit();

  std::unordered_set<oid_t> tile_groups_to_sync_;
};

}  // namespace logging
}  // namespace peloton
