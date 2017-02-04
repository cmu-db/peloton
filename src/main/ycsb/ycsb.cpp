//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb.cpp
//
// Identification: src/main/ycsb/ycsb.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//w

#include <iostream>
#include <fstream>
#include <iomanip>

#include "common/logger.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_workload.h"

#include "gc/gc_manager_factory.h"
#include "concurrency/epoch_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;
// Main Entry Point
void RunBenchmark() {

  if (state.gc_mode == true) {
    gc::GCManagerFactory::Configure(state.gc_backend_count);
  }
  
  std::unique_ptr<std::thread> epoch_thread;
  std::vector<std::unique_ptr<std::thread>> gc_threads;

  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
  gc::GCManager &gc_manager = gc::GCManagerFactory::GetInstance();

  // start epoch.
  // epoch_manager.StartEpoch();
  
  // start GC.
  gc_manager.StartGC();

  // Create the database
  CreateYCSBDatabase();

  // Load the databases
  LoadYCSBDatabase();

  // Run the workload
  RunWorkload();
  
  // stop GC.
  gc_manager.StopGC();

  // stop epoch.
  // epoch_manager.StopEpoch();

  // Emit throughput
  WriteOutput();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::ycsb::ParseArguments(argc, argv,
                                           peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
