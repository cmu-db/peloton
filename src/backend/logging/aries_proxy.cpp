/*-------------------------------------------------------------------------
 *
 * aries_proxy.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/aries_proxy.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/logger.h"
#include "backend/logging/aries_proxy.h"

#include <iostream> // cout
#include <fstream>
#include <unistd.h> // sleep

namespace peloton {
namespace logging {

void AriesProxy::logging_MainLoop() const{
  // TODO :: performance optimization..
  for(int i=0;i<50;i++){
    sleep(5);
    //printf("buffer size %u GetBufferSize() %d \n", buffer_size,(int) GetBufferSize());
    if( GetBufferSize() >= buffer_size ) flush();
  }
}

/**
 * @brief Recording log record
 * @param log record 
 */
void AriesProxy::log(LogRecord record) const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  aries_buffer.push_back(record);
}

/**
 * @brief Get buffer size
 * @return return the size of buffer
 */
size_t AriesProxy::GetBufferSize() const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  return aries_buffer.size();
}

/**
 * TODO :: it should flush the log into file .. and mark the commit the end of the log file
 * @brief flush all record, for now it's just printing out
 */
void AriesProxy::flush() const{
  std::lock_guard<std::mutex> buffer_lock(aries_buffer_mutex);
  std::lock_guard<std::mutex> log_file_lock(aries_log_file_mutex);

  std::ofstream log_file;
  //FIXME :: Hard coded path
  log_file.open("/home/parallels/git/peloton/build/data/aries_log_file.log", std::ios::out | std::ios::app);

  std::cout << "\n::StartFlush::\n";
  log_file << "::StartFlush::\n";
  for( auto record : aries_buffer ){
    std::cout << record;
    log_file << record;
  }
  std::cout << "::Commit::" << std::endl;
  log_file << "::Commit::\n";
  aries_buffer.clear();
  log_file.close();
}

}  // namespace logging
}  // namespace peloton


