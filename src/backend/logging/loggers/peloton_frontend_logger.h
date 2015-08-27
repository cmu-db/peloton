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

static std::vector<LogRecord*> peloton_global_queue;

//===--------------------------------------------------------------------===//
// Peloton Frontend Logger 
//===--------------------------------------------------------------------===//

class PelotonFrontendLogger : public FrontendLogger{

  public:

    PelotonFrontendLogger(void);

   ~PelotonFrontendLogger(void);

    void MainLoop(void);

    void CollectLogRecord(void);

    void Flush(void);

    void CollectCommittedTuples(TupleRecord* record,
                                std::vector<ItemPointer> &inserted_tuples,
                                std::vector<ItemPointer> &deleted_tuples);

    //===--------------------------------------------------------------------===//
    // Recovery 
    //===--------------------------------------------------------------------===//

    void Recovery(void);

    LogRecordType GetNextLogRecordType(void);

    void JumpToLastUnfinishedTxn(void);

    size_t LogFileSize();

    bool IsFileBroken(size_t size_to_read);

    size_t GetNextFrameSize(void);

    bool ReadTxnRecord(TransactionRecord &txnRecord);

    bool ReadTupleRecordHeader(TupleRecord& tupleRecord);

    void InsertTuple(void);

    void DeleteTuple(void);

    void UpdateTuple(void);

    void SkipTxnRecord(LogRecordType log_record_type);

    void SetInsertCommit(ItemPointer location, bool commit);

    void SetDeleteCommit(ItemPointer location, bool commit);

    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // FIXME :: Hard coded file name
    std::string filename = "peloton.log";

    // File pointer and descriptor
    FILE* logFile;

    int logFileFd;
};

}  // namespace logging
}  // namespace peloton
