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
#include "backend/storage/backend_nvm.h"

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
    void CollectCommittedTuples(TupleRecord* record);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void DoRecovery(void);

    cid_t SetInsertCommitMark(ItemPointer location);

    cid_t SetDeleteCommitMark(ItemPointer location);

    //===--------------------------------------------------------------------===//
    // Utility functions
    //===--------------------------------------------------------------------===//

    void CommitRecords(LogRecordList *txn_log_record_list);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // Global queue
    static LogRecordPool *global_plog_pool;
    storage::NVMBackend *backend;

    // Keep tracking max oid for setting next_oid in manager
    // For active processing after recovery
    oid_t max_oid = INVALID_OID;
    // Keep tracking latest cid for setting next commit in txn manager
    cid_t latest_cid = INVALID_CID;
};

}  // namespace logging
}  // namespace peloton
