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
  for(int i=0;i<50;i+=2){
    sleep(2);
    if( GetBufferSize() > 2 ) Flush();
  }
}

void AriesProxy::log(LogRecord record) const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  aries_buffer.push_back(record);
}

size_t AriesProxy::GetBufferSize() const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  return aries_buffer.size();
}

void AriesProxy::Flush() const{
  std::lock_guard<std::mutex> lock(aries_buffer_mutex);
  for( auto record : aries_buffer )
    std::cout << "record : " << record << std::endl;
  aries_buffer.clear();
}

}  // namespace logging
}  // namespace peloton


