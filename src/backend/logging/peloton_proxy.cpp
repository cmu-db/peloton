/*-------------------------------------------------------------------------
 *
 * peloton_proxy.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/peloton_proxy.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/peloton_proxy.h"

#include <iostream> // cout
#include <unistd.h> // sleep

namespace peloton {
namespace logging {

void PelotonProxy::logging_MainLoop() const{

  // TODO :: performance optimization
  for(int i=0;i<50;i+=2){
    sleep(2);
    if( GetBufferSize() > 2 ) Flush();
  }
}

void PelotonProxy::log(LogRecord record) const{
  std::lock_guard<std::mutex> lock(peloton_buffer_mutex);
  peloton_buffer.push_back(record);
}

size_t PelotonProxy::GetBufferSize() const{
  std::lock_guard<std::mutex> lock(peloton_buffer_mutex);
  return peloton_buffer.size();
}

void PelotonProxy::Flush() const{
  std::lock_guard<std::mutex> lock(peloton_buffer_mutex);
  for( auto record : peloton_buffer )
    std::cout << "record : " << record << std::endl;
  peloton_buffer.clear();
}

}  // namespace logging
}  // namespace peloton


