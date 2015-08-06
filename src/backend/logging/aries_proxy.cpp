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

#include "backend/logging/aries_proxy.h"

#include <iostream> // cout
#include <unistd.h> // sleep

namespace peloton {
namespace logging {

void AriesProxy::logging_MainLoop() const{
  // TODO :: performance optimization
  for(int i=0;i<50;i++){
    sleep(5);
    printf("buffer size %u GetBufferSize() %d \n", buffer_size,(int) GetBufferSize());
    if( GetBufferSize() >= buffer_size ) Flush();
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
 * TODO ::
 * @brief flush all record, for now it's just printing out
 */
void AriesProxy::Flush() const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  for( auto record : aries_buffer )
    std::cout << "record : " << record << std::endl;
  aries_buffer.clear();
}

}  // namespace logging
}  // namespace peloton


