//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.h
//
// Identification: src/include/common/init.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/thread_pool.h"

namespace peloton {

extern ThreadPool thread_pool;

//===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

class PelotonInit {
 public:

  static void Initialize();

  static void Shutdown();

  static void SetUpThread();

  static void TearDownThread();

};

}  // End peloton namespace
