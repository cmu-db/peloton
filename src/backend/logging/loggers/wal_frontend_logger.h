/*-------------------------------------------------------------------------
 *
 * wal_frontend_logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/wal_frontend_logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontend_logger.h"

namespace peloton {

class VarlenPool;

namespace concurrency {
class Transaction;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Write Ahead Frontend Logger
//===--------------------------------------------------------------------===//

class WriteAheadFrontendLogger : public FrontendLogger {
 public:
  WriteAheadFrontendLogger(void);

  ~WriteAheadFrontendLogger(void);

  void FlushLogRecords(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void AddTransactionToRecoveryTable(void);

  void RemoveTransactionFromRecoveryTable(void);

  void MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction *recovery_txn);

  void AbortTuplesFromRecoveryTable(void);

  void MoveTuples(concurrency::Transaction *destination,
                  concurrency::Transaction *source);

  void InsertTuple(concurrency::Transaction *recovery_txn);

  void DeleteTuple(concurrency::Transaction *recovery_txn);

  void UpdateTuple(concurrency::Transaction *recovery_txn);

  void AbortActiveTransactions();

  void AbortTuples(concurrency::Transaction *txn);

 private:
  std::string GetLogFileName(void);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // File pointer and descriptor
  FILE *log_file;
  int log_file_fd;

  // Size of the log file
  size_t log_file_size;

  // Txn table during recovery
  std::map<txn_id_t, concurrency::Transaction *> recovery_txn_table;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool;
};

}  // namespace logging
}  // namespace peloton
