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
#include "backend/concurrency/transaction_manager_factory.h"

#include "unordered_set"

extern char *peloton_endpoint_address;

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

  WriteBehindBackendLogger() {
    if (peloton_endpoint_address != nullptr) {
      replicating_ = true;
    }
    logging_type = LOGGING_TYPE_NVM_WBL;
  }

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

  bool replicating_ = false;
  std::unordered_set<oid_t> tile_groups_to_sync_;
};

}  // namespace logging
}  // namespace peloton
