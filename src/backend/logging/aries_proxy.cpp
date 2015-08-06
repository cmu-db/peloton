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
    if( GetBufferSize() > buffer_size ) Flush();
  }
}

/**
 * @brief Recording log record
 * @param log record 
 */
void AriesProxy::log(LogRecord record) const{
  aries_buffer_mutex.lock();
  aries_buffer.push_back(record);
  aries_buffer_mutex.unlock();
}

/**
 * @brief Get buffer size
 * @return return the size of buffer
 */
size_t AriesProxy::GetBufferSize() const{
  aries_buffer_mutex.lock();
  size_t size = aries_buffer.size();
  aries_buffer_mutex.unlock();
  return size;
}

/**
 * TODO ::
 * @brief flush all record, for now it's just printing out
 */
void AriesProxy::Flush() const{
  aries_buffer_mutex.lock();
  for( auto record : aries_buffer )
    std::cout << "record : " << record << std::endl;
  aries_buffer.clear();
  aries_buffer_mutex.unlock();
}

}  // namespace logging
}  // namespace peloton


