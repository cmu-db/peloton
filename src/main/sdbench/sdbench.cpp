//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench.cpp
//
// Identification: src/main/sdbench/sdbench.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "common/logger.h"
#include "benchmark/sdbench/sdbench_configuration.h"
#include "benchmark/sdbench/sdbench_workload.h"
#include "benchmark/sdbench/sdbench_loader.h"
#include "concurrency/epoch_manager_factory.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace benchmark {
namespace sdbench {

configuration state;

// Main Entry Point
void RunBenchmark() {
  concurrency::EpochManagerFactory::Configure(EpochType::DECENTRALIZED_EPOCH);

  std::unique_ptr<std::thread> epoch_thread;

  concurrency::EpochManager &epoch_manager =
      concurrency::EpochManagerFactory::GetInstance();

  epoch_manager.RegisterThread(0);

  epoch_manager.StartEpoch(epoch_thread);

  if (state.multi_stage) {
    // Run holistic indexing comparison benchmark
    RunMultiStageBenchmark();
  } else {
    // Run a single sdbench test
    RunSDBenchTest();
  }

  epoch_manager.StopEpoch();

  epoch_thread->join();
}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::sdbench::ParseArguments(
      argc, argv, peloton::benchmark::sdbench::state);

  peloton::benchmark::sdbench::RunBenchmark();

  peloton::benchmark::sdbench::sdbench_table.release();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
