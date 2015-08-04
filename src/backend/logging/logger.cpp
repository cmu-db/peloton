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

#include "logger.h"

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
    sleep(3);
    AddQueue(i);
    if( i == 20 ) Flush();
  }
}

void Logger::AddQueue(int element){
  std::lock_guard<std::mutex> lock(queue_mutex);
  queue.push_back(element);
}

void Logger::Flush(){
  std::lock_guard<std::mutex> lock(queue_mutex);
  for( auto element : queue )
    std::cout << "element : " << element << std::endl;
  queue.clear();
}

}  // namespace logging
}  // namespace peloton


