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

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Logger 
//===--------------------------------------------------------------------===//

// queue
static std::vector<int> queue;

static std::mutex queue_mutex;

class Logger{

  public:

    Logger() {}

    static Logger& GetInstance();

    void logging_MainLoop(void);

    void AddQueue(int queue);

    void Flush();
 
};

}  // namespace logging
}  // namespace peloton
