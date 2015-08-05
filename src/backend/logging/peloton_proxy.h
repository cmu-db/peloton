/*-------------------------------------------------------------------------
 *
 * peloton_proxy.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/peloton_proxy.h
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
// Peloton Proxy
//===--------------------------------------------------------------------===//

// log queue
static std::vector<LogRecord> peloton_buffer;

static std::mutex peloton_buffer_mutex;

class PelotonProxy : public LogProxy{

  public:

    PelotonProxy() {}

    void logging_MainLoop(void) const;

    void log(LogRecord records) const;

  private:

    size_t GetBufferSize() const;

    void Flush() const;
 
};

}  // namespace logging
}  // namespace peloton
