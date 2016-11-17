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
#include "common/thread_pool.h"
#include "common/config.h"

#include "gc/gc_manager_factory.h"
#include "concurrency/epoch_manager_factory.h"
#include "storage/data_table.h"

#include "libcds/cds/init.h"

#include <google/protobuf/stubs/common.h>

#include <thread>

namespace peloton {

ThreadPool thread_pool;

void PelotonInit::Initialize() {

  // Initialize CDS library
  cds::Initialize();


  QUERY_THREAD_COUNT = std::thread::hardware_concurrency();
  LOGGING_THREAD_COUNT = 1;
  GC_THREAD_COUNT = 1;
  EPOCH_THREAD_COUNT = 1;

  // set max thread number.
  thread_pool.Initialize(0, std::thread::hardware_concurrency() + 3);

  int parallelism = (std::thread::hardware_concurrency() + 1) / 2;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);


  // start epoch.
  concurrency::EpochManagerFactory::GetInstance().StartEpoch();
  // start GC.
  gc::GCManagerFactory::GetInstance().StartGC();
}

void PelotonInit::Shutdown() {

  // shut down GC.
  gc::GCManagerFactory::GetInstance().StopGC();
  // shut down epoch.
  concurrency::EpochManagerFactory::GetInstance().StopEpoch();

  thread_pool.Shutdown();

  // Terminate CDS library
  cds::Terminate();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  // Shut down GFLAGS.
  ::google::ShutDownCommandLineFlags();
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
