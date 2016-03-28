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

/*-------------------------------------------------------------------------
 *
 * log_record_pool.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/records/log_record_pool.h
 *
 *-------------------------------------------------------------------------
 */

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
 public:
  //===--------------------------------------------------------------------===//
  // Accessor
  //===--------------------------------------------------------------------===//
  void Clear();

  bool IsEmpty() const { return txn_log_table.empty(); }

  int CreateTransactionLogList(txn_id_t txn_id);

  int AddLogRecord(TupleRecord *record);

  void RemoveTransactionLogList(txn_id_t txn_id);

  std::vector<TupleRecord *> *SearchLogRecordList(txn_id_t txn_id);

 private:
  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//
  // Transient record for fast access to log records
  std::map<txn_id_t, std::vector<TupleRecord *>> txn_log_table;
};

}  // namespace logging
}  // namespace peloton
