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

#include <string>
#pragma once

namespace peloton {

class ThreadPool;

extern ThreadPool thread_pool;

//===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

class PelotonInit {
 public:
  static void Initialize(std::string log_dir = "/tmp", std::string log_file = "log_file");

  static void Shutdown();

  static void SetUpThread();

  static void TearDownThread();
};

}  // namespace peloton
