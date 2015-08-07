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

#include "backend/logging/logger/stdoutfrontendlogger.h"

namespace peloton {
namespace logging {

void StdoutFrontendLogger::MainLoop(void) const {
  for(int i=0;;i++){
    sleep(5);
    printf("buffer size %u GetBufferSize() %d \n", buffer_size,(int) GetBufferSize());
    if( GetBufferSize() >= buffer_size ) flush();
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void StdoutFrontendLogger::flush(void) const {
  std::lock_guard<std::mutex> buffer_lock(stdout_buffer_mutex);

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
size_t StdoutFrontendLogger::GetBufferSize() const{
  std::lock_guard<std::mutex> lock(stdout_buffer_mutex);
  return stdout_buffer.size();
}

}  // namespace logging
}  // namespace peloton
