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
#include "catalog/catalog.h"

#include "common/init.h"
#include "common/thread_pool.h"
#include "common/config.h"

#include "gc/gc_manager_factory.h"
#include "storage/data_table.h"

#include "libcds/cds/init.h"

#include <google/protobuf/stubs/common.h>

#include <thread>

namespace peloton {

ThreadPool thread_pool;

void PelotonInit::Initialize() {

  // Initialize CDS library
  cds::Initialize();

  // FIXME: A number for the available threads other than
  // std::thread::hardware_concurrency() should be
  // chosen. Assigning new task after reaching maximum will
  // block.
  thread_pool.Initialize(std::thread::hardware_concurrency(),
                         std::thread::hardware_concurrency());

  int parallelism = (std::thread::hardware_concurrency() + 1) / 2;
  storage::DataTable::SetActiveTileGroupCount(parallelism);
  storage::DataTable::SetActiveIndirectionArrayCount(parallelism);

  // the garbage collector is assigned to dedicated threads.
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  gc_manager.StartGC();

  //initialize the catalog so we don't do this on the first query
  catalog::Catalog::GetInstance();
}

void PelotonInit::Shutdown() {

  // shut down GC.
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  gc_manager.StopGC();

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
