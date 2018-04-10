//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// utility.h
//
// Identification: src/include/common/utility.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"

namespace peloton{

  int peloton_close(int fd, int failure_log_level = LOG_LEVEL_DEBUG);
}
