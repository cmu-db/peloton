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

namespace peloton {
namespace logging {

// TODO: Should this be global ?
static std::vector<LogRecord*> peloton_global_queue;

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger {

  public:

    PelotonFrontendLogger(void);

   ~PelotonFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void);

    // Used by flush to update the commit mark
    void CollectCommittedTuples(TupleRecord* record,
                                std::vector<ItemPointer> &inserted_tuples,
                                std::vector<ItemPointer> &deleted_tuples);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void DoRecovery(void);

    void SkipTransactionRecord(LogRecordType log_record_type);

    void SetInsertCommitMark(ItemPointer location, bool commit);

    void SetDeleteCommitMark(ItemPointer location, bool commit);

    void InsertTuple(void);

    void DeleteTuple(void);

    void UpdateTuple(void);

    //===--------------------------------------------------------------------===//
    // Utility functions
    //===--------------------------------------------------------------------===//

    size_t GetLogFileSize();

    bool IsFileTruncated(size_t size_to_read);

    size_t GetNextFrameSize(void);

    LogRecordType GetNextLogRecordType(void);

    void JumpToLastActiveTransaction(void);

    bool ReadTransactionRecordHeader(TransactionRecord &txn_record);

    bool ReadTupleRecordHeader(TupleRecord& tuple_record);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // TODO :: Hard coded file name
    std::string file_name = "peloton.log";

    // File pointer and descriptor
    FILE* log_file;
    int log_file_fd;

};

}  // namespace logging
}  // namespace peloton
