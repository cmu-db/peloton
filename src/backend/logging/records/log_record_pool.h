//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record_pool.h
//
// Identification: src/backend/logging/records/log_record_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <vector>

#include "backend/logging/records/tuple_record.h"

namespace peloton {
namespace logging {
//===--------------------------------------------------------------------===//
// Log record pool
//===--------------------------------------------------------------------===//

class LogRecordPool {
  friend class WriteBehindFrontendLogger;

 public:
  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//
  void Clear();

  bool IsEmpty() const { return txn_log_table.empty(); }

  int CreateTxnLogList(txn_id_t txn_id);

  int AddLogRecord(std::unique_ptr<TupleRecord> record);

  void RemoveTxnLogRecordList(txn_id_t txn_id);

  bool ExistsTxnLogRecordList(txn_id_t txn_id);

 private:
  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//
  // Transient record for fast access to log records
  std::map<txn_id_t, std::vector<std::unique_ptr<TupleRecord>>> txn_log_table;
};

}  // namespace logging
}  // namespace peloton
