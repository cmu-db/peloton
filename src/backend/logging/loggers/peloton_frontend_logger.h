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

#include "backend/logging/frontend_logger.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/log_record_pool.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger {

  public:

    PelotonFrontendLogger(void);

   ~PelotonFrontendLogger(void);

    void Flush(void);

    // Used by flush to update the commit mark
    bool CollectCommittedTuples(TupleRecord* record);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void DoRecovery(void);

    cid_t SetInsertCommitMark(ItemPointer location);

    cid_t SetDeleteCommitMark(ItemPointer location);

    //===--------------------------------------------------------------------===//
    // Utility functions
    //===--------------------------------------------------------------------===//

    void FlushRecords(std::vector<txn_id_t> committing_list);

    void CommitRecords(std::vector<txn_id_t> committing_list);

  private:
    std::string GetLogFileName(void);

    void WriteTxnLog(TransactionRecord txnLog);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // File pointer and descriptor
    FILE* log_file;
    int log_file_fd;

    // Global pool
    static LogRecordPool global_plog_pool;

    // Keep tracking max oid for setting next_oid in manager
    // For active processing after recovery
    oid_t max_oid = INVALID_OID;
    // Keep tracking latest cid for setting next commit in txn manager
    cid_t latest_cid = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
