/*-------------------------------------------------------------------------
 *
 * utils.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/utils.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include <execinfo.h>
#include <errno.h>
#include <cxxabi.h>
#include <signal.h>

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
//  Bridge Utils
//===--------------------------------------------------------------------===//

void PrintStackTrace();

} // namespace bridge
} // namespace peloton
