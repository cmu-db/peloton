/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger 
//===--------------------------------------------------------------------===//

class Logger{
  public:
    static void logging_MainLoop(void);
//    Logger(oid_t database_oid,
//           oid_t buffer_size,
//           void* nvram_ptr);
//
//    /* db oid */
//    /* buffer size(# of logs) to commit (default : 1) */
//    /* nvm */
//
//    //Make tuple template 
//    // Pointer? deep copy needed ,, isn't it?
//    bool Record(const concurrency::Transaction *transaction, 
//                const storage::Tuple *tuple)
//    {
//      {
//        std::lock_guard<std::mutex> lock(table_mutex);
//
//        txn_buffer.push_back(transaction);
//
//        // flush when buffer is full
//        if( txn_buffer.size() == buffer_size ){
//          //allocate memory in NVRAM
//        }
//      }
//    }
  private:
    // txn vector
    //std::vector<concurrency::Transaction*> txn_buffer;

    // nvram pointer

    // buffer size

    // logger mutex
    //std::mutex logger_mutex;
};

}  // namespace logging
}  // namespace peloton
