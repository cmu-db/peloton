/*-------------------------------------------------------------------------
 *
 * aries_proxy.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/aries_proxy.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/logging/logproxy.h"
#include "backend/logging/logrecord.h"

#include <mutex>
#include <vector>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Aries Proxy
//===--------------------------------------------------------------------===//

// log queue
static std::vector<LogRecord> aries_buffer;

static std::mutex aries_buffer_mutex;

static std::mutex aries_log_file_mutex;

class AriesProxy : public LogProxy{

  public:

    // Default buffer size is 0, which means logger is 'LOG ONLY'
    // not for running main loop
    // TODO :: file name 
    AriesProxy(oid_t buffer_size = 0 /*LOG ONLY*/)  
    : buffer_size(buffer_size){}

    void logging_MainLoop(void) const;

    void log(LogRecord records) const;

    void flush() const;

  private:
    oid_t buffer_size;
    
    size_t GetBufferSize() const;
};

}  // namespace logging
}  // namespace peloton
