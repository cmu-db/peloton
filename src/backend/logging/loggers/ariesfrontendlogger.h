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

#include "backend/logging/frontendlogger.h"
#include "backend/logging/records/tuplerecord.h"
#include "backend/logging/records/transactionrecord.h"
#include "backend/concurrency/transaction.h"

#include <fcntl.h>

namespace peloton {
namespace logging {

static std::vector<LogRecord*> aries_global_queue;

//===--------------------------------------------------------------------===//
// Aries Frontend Logger 
//===--------------------------------------------------------------------===//

class AriesFrontendLogger : public FrontendLogger{

  public:

    AriesFrontendLogger(void);

   ~AriesFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void Recovery(void);

  private:

    LogRecordType GetNextLogRecordType(void);

    size_t LogFileSize();

    bool IsFileBroken(size_t size_to_read);

    size_t GetNextFrameSize(void);

    size_t GetLogRecordCount() const;

    bool ReadTxnRecord(TransactionRecord &txnRecord);

    bool ReadTupleRecordHeader(TupleRecord& tupleRecord);

    storage::Tuple* ReadTupleRecordBody(catalog::Schema* schema,
                                        Pool *pool);

    storage::DataTable* GetTable(TupleRecord tupleRecord);

    storage::TileGroup* GetTileGroup(oid_t tile_group_id);

    void MoveTuples(concurrency::Transaction* destination,
                    concurrency::Transaction* source);

    void AbortTuples(concurrency::Transaction* txn);

    void AbortTxnInRecoveryTable();

    void InsertTuple(concurrency::Transaction* recovery_txn);
    void DeleteTuple(concurrency::Transaction* recovery_txn);
    void UpdateTuple(concurrency::Transaction* recovery_txn);

    void AddTxnToRecoveryTable(void);
    void RemoveTxnFromRecoveryTable(void);
    void MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction* recovery_txn);
    void AbortTuplesFromRecoveryTable(void);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // FIXME :: Hard coded file name
    std::string filename = "aries.log";

    // File pointer and descriptor
    FILE* logFile;

    int logFileFd;

    // Txn table
    std::map<txn_id_t, concurrency::Transaction *> recovery_txn_table;

    // Keep tracking max oid for setting next_oid in manager 
    oid_t max_oid = INVALID_OID;
};

}  // namespace logging
}  // namespace peloton
