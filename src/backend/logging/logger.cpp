/*-------------------------------------------------------------------------
 *
 * logger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/logger.h"

#include <iostream> // cout
#include <unistd.h> // sleep

namespace peloton {
namespace logging {

Logger& Logger::GetInstance()
{
  static Logger logger;
  return logger;
}

void Logger::logging_MainLoop(){

  // TODO :: performance optimization
  for(int i=0; ;i+=2){
    sleep(1);
    LogRecord record(LOG_TYPE_DEFAULT, i);
    Record(record);
    if( GetBufferSize() > 10 ) Flush();
  }
}

void Logger::Record(LogRecord record){
  std::lock_guard<std::mutex> lock(queue_mutex);
  queue.push_back(record);
}

size_t Logger::GetBufferSize() const {
  std::lock_guard<std::mutex> lock(queue_mutex);
  return queue.size();
}

void Logger::Flush(){
  std::lock_guard<std::mutex> lock(queue_mutex);
  for( auto record : queue )
    std::cout << "element : " << record << std::endl;
  queue.clear();
}

}  // namespace logging
}  // namespace peloton


