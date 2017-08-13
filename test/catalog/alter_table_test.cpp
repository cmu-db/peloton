//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_table_test.cpp
//
// Identification: test/performance/alter_table_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/platform.h"
#include "common/timer.h"
#include "index/index_factory.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {
