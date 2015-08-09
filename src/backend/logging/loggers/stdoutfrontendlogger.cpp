/*-------------------------------------------------------------------------
 *
 * stdoutfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/stdoutfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/stdoutfrontendlogger.h"

namespace peloton {
namespace logging {

void StdoutFrontendLogger::MainLoop(void) const {
  for(int i=0;;i++){
    sleep(5);
    printf("buffer size %u GetLogCount() %d \n", buffer_size,(int) GetLogCount());
    if( GetLogCount() >= buffer_size ) commit();
  }
}

/**
 * @brief commit all record, for now it's just printing out
 */
void StdoutFrontendLogger::commit(void) const {
  std::cout << "\n::StartFlush::\n";

  for( auto record : stdout_buffer ){
    std::cout << record;
  }
  std::cout << "::Commit::" << std::endl;
  stdout_buffer.clear();
}

/**
 * @brief Get buffer size
 * @return return the size of buffer
 */
size_t StdoutFrontendLogger::GetLogCount() const{
  return stdout_buffer.size();
}

void StdoutFrontendLogger::CollectLogRecords(void){
  std::vector<BackendLogger*> backend_loggers = GetBackendLoggers();

  // Look over backend loggers' local_commit_offset
  for(auto backend_logger : backend_loggers){
    auto local_commit_offset = backend_logger->GetLocalCommitOffset();
    std::cout << "lco : " << local_commit_offset << std::endl;
    // How to lock the be logger's lock
    // if it's has valid value, lock that vector and move
    // into frontend vector
    // set the local commit offset to invalid_oid
    //auto& queue = backend_logger->Lock()?
  }
}

}  // namespace logging
}  // namespace peloton
