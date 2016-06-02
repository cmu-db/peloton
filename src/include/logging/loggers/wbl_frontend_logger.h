//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wbl_frontend_logger.h
//
// Identification: src/logging/loggers/wbl_frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <set>

#include "logging/frontend_logger.h"
#include "logging/records/transaction_record.h"
#include "logging/records/tuple_record.h"
#include "logging/records/log_record_pool.h"

namespace peloton {

namespace storage {
class TileGroupHeader;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Write Behind Frontend Logger
//===--------------------------------------------------------------------===//

class WriteBehindFrontendLogger : public FrontendLogger {
 public:
  WriteBehindFrontendLogger(void);

  ~WriteBehindFrontendLogger(void);

  void FlushLogRecords(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void SetLoggerID(int);

  void RecoverIndex(){};

  std::string GetLogFileName();

  static constexpr auto wbl_log_path = "wbl.log";

 private:
  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  CopySerializeOutput output_buffer;

  // File pointer and descriptor
  FILE *log_file;
  int log_file_fd;

  // Size of the log file
  size_t log_file_size;

  // For now don't do anything with this
  oid_t max_oid = INVALID_OID;

  // Keep tracking latest cid for setting next commit in txn manager
  cid_t max_commit_id_seen = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
