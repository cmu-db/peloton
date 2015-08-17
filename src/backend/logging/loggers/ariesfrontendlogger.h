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

    size_t GetNextFrameSize(void);

    size_t GetLogRecordCount() const;

    void ReadTxnRecord(TransactionRecord &txnRecord);

    bool ReadTupleRecordHeader(TupleRecord& tupleRecord);

    storage::Tuple* ReadTupleRecordBody(catalog::Schema* schema);

    storage::DataTable* GetTable(TupleRecord tupleRecord);

    storage::TileGroup* GetTileGroup(oid_t tile_group_id);

    void MoveTuples(concurrency::Transaction* destination,
                    concurrency::Transaction* source);

    void AbortTuples(concurrency::Transaction* txn);

    void AbortRemainedTxnInRecoveryTable();

    void InsertTuple(concurrency::Transaction* recovery_txn);
    void DeleteTuple(concurrency::Transaction* recovery_txn);
    void UpdateTuple(concurrency::Transaction* recovery_txn);

    // TODO :: Rename
    void AddTxnToRecoveryTable(void);
    void RemoveTxnFromRecoveryTable(void);

    void CommitTuplesFromRecoveryTable(concurrency::Transaction* recovery_txn);
    void AbortTuplesFromRecoveryTable(void);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // FIXME :: Hard coded file name
    std::string filename = "/home/parallels/git/peloton/build/aries.log";

    // FIXME :: Hard coded global_queue size
    oid_t aries_global_queue_size = 1;

    // File pointer and descriptor
    FILE* logFile;

    int logFileFd;

    // permit reading and writing by the owner, and to permit reading
    // only by group members and others.
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

    // Txn table
    std::map<txn_id_t, concurrency::Transaction *> txn_table;

    // Keep tracking max oid for setting next_oid in manager 
    oid_t max_oid = INVALID_OID;
};

}  // namespace logging
}  // namespace peloton
