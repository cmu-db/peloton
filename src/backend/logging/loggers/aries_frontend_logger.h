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
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/concurrency/transaction.h"

namespace peloton {
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

  //===--------------------------------------------------------------------===//
  // Utility functions
  //===--------------------------------------------------------------------===//

  size_t GetLogFileSize();

  bool IsFileTruncated(size_t size_to_read);

  size_t GetNextFrameSize(void);

  size_t GetLogRecordCount() const;

  LogRecordType GetNextLogRecordType(void);

  bool ReadTransactionRecordHeader(TransactionRecord &txn_record);

  bool ReadTupleRecordHeader(TupleRecord& tuple_record);

  storage::Tuple* ReadTupleRecordBody(catalog::Schema* schema,
                                      Pool *pool);

  // Wrappers
  storage::DataTable* GetTable(TupleRecord tupleRecord);

  storage::TileGroup* GetTileGroup(oid_t tile_group_id);

 private:

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // FIXME :: Hard coded file name
  std::string file_name = "aries.log";

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
