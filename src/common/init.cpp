//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.cpp
//
// Identification: src/common/init.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/init.h"

#include "libcds/cds/init.h"

#include <google/protobuf/stubs/common.h>

#include <thread>

namespace peloton {

WorkerThreadPool thread_pool;

void PelotonInit::Initialize() {

  // Initialize CDS library
  cds::Initialize();

  thread_pool.Initialize(std::thread::hardware_concurrency());
}

void PelotonInit::Shutdown() {

  // Terminate CDS library
  cds::Terminate();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();
}

void PelotonInit::SetUpThread() {

  // Attach thread to cds
  cds::threading::Manager::attachThread();
}

void PelotonInit::TearDownThread() {

  // Detach thread from cds
  cds::threading::Manager::detachThread();
}

}  // End peloton namespace
