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

#include "benchmark/sdbench/sdbench_configuration.h"
#include "common/logger.h"
#include "benchmark/sdbench/sdbench_workload.h"
#include "benchmark/sdbench/sdbench_loader.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace benchmark {
namespace sdbench {

configuration state;

// Main Entry Point
void RunBenchmark() {
  // Initialize settings
  peloton_layout_mode = state.layout_mode;

  // Generate sequence
  GenerateSequence(state.column_count);

  // Single run
  if (state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    CreateAndLoadTable((LayoutType)peloton_layout_mode);

    switch (state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        RunDirectTest();
        break;

      case OPERATOR_TYPE_INSERT:
        RunInsertTest();
        break;

      default:
        LOG_ERROR("Unsupported test type : %d", state.operator_type);
        break;
    }

  }
  // Experiment
  else {
    switch (state.experiment_type) {

      case EXPERIMENT_TYPE_ADAPT:
        RunAdaptExperiment();
        break;

      default:
        LOG_ERROR("Unsupported experiment_type : %d", state.experiment_type);
        break;
    }
  }
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
