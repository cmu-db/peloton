/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <set>

#include "backend/logging/frontend_logger.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/log_record_pool.h"

namespace peloton {

namespace storage{
class TileGroupHeader;
}

namespace logging {

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger {

  public:

    PelotonFrontendLogger(void);

   ~PelotonFrontendLogger(void);

    void FlushLogRecords(void);

    // Collect the tuple log records for flushing
    std::pair<bool,ItemPointer> CollectTupleRecord(TupleRecord* record);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void DoRecovery(void);

    storage::TileGroupHeader *SetInsertCommitMark(ItemPointer location);

    storage::TileGroupHeader *SetDeleteCommitMark(ItemPointer location);

    //===--------------------------------------------------------------------===//
    // Utility functions
    //===--------------------------------------------------------------------===//

    size_t WriteLogRecords(std::vector<txn_id_t> committing_list);

    std::set<storage::TileGroupHeader*> ToggleCommitMarks(std::vector<txn_id_t> committing_list);

    void SyncTileGroups(std::set<oid_t> tile_group_set);

    void SyncTileGroupHeaders(std::set<storage::TileGroupHeader*> tile_group_header_set);

  private:
    std::string GetLogFileName(void);

    bool NeedRecovery(void);

    void WriteTransactionLogRecord(TransactionRecord txnLog);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    CopySerializeOutput output_buffer;

    // File pointer and descriptor
    FILE* log_file;
    int log_file_fd;

    // Size of the log file
    size_t log_file_size;

    // Global pool
    LogRecordPool global_peloton_log_record_pool;

    // Keep tracking max oid for setting next_oid in manager
    // For active processing after recovery
    oid_t max_oid = INVALID_OID;
    // Keep tracking latest cid for setting next commit in txn manager
    cid_t latest_commit_id = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
