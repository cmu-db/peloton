//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_pool.h
//
// Identification: src/include/common/worker_thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <include/common/logger.h>
namespace peloton{


  int peloton_close(int fd, int failure_log_level = LOG_LEVEL_DEBUG);
}
