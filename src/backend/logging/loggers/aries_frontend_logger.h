/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/frontend_logger.h"

namespace peloton {

namespace concurrency{
class Transaction;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Aries Frontend Logger 
//===--------------------------------------------------------------------===//

class AriesFrontendLogger : public FrontendLogger{

 public:

  AriesFrontendLogger(void);

  ~AriesFrontendLogger(void);

  void Flush(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void AddTransactionToRecoveryTable(void);

  void RemoveTransactionFromRecoveryTable(void);

  void MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction* recovery_txn);

  void AbortTuplesFromRecoveryTable(void);

  void MoveTuples(concurrency::Transaction* destination,
                  concurrency::Transaction* source);

  void InsertTuple(concurrency::Transaction* recovery_txn);

  void DeleteTuple(concurrency::Transaction* recovery_txn);

  void UpdateTuple(concurrency::Transaction* recovery_txn);

  void AbortActiveTransactions();

  void AbortTuples(concurrency::Transaction* txn);

 private:

  std::string GetLogFileName(void);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // File pointer and descriptor
  FILE* log_file;
  int log_file_fd;

  // Txn table during recovery
  std::map<txn_id_t, concurrency::Transaction *> recovery_txn_table;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;

};

}  // namespace logging
}  // namespace peloton
