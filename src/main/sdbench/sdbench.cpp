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

namespace peloton {
namespace benchmark {
namespace sdbench {

configuration state;

// Main Entry Point
void RunBenchmark() {
  // Initialize settings
  peloton_layout_mode = state.layout_mode;
  peloton_projectivity = state.projectivity;

  // Generate sequence
  GenerateSequence(state.column_count);

  // Single run
  if (state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    CreateAndLoadTable((LayoutType)peloton_layout_mode);

    switch (state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        RunDirectTest();
        break;

      case OPERATOR_TYPE_AGGREGATE:
        RunAggregateTest();
        break;

      case OPERATOR_TYPE_ARITHMETIC:
        RunArithmeticTest();
        break;

      case OPERATOR_TYPE_JOIN:
        RunJoinTest();
        break;

      default:
        LOG_ERROR("Unsupported test type : %d", state.operator_type);
        break;
    }

  }
  // Experiment
  else {
    switch (state.experiment_type) {
      case EXPERIMENT_TYPE_PROJECTIVITY:
        RunProjectivityExperiment();
        break;

      case EXPERIMENT_TYPE_SELECTIVITY:
        RunSelectivityExperiment();
        break;

      case EXPERIMENT_TYPE_OPERATOR:
        RunOperatorExperiment();
        break;

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

  return 0;
}
