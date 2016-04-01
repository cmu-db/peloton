//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyadapt.cpp
//
// Identification: src/backend/benchmark/hyadapt/hyadapt.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/benchmark/hyadapt/hyadapt_configuration.h"
#include "backend/common/logger.h"
#include "backend/benchmark/hyadapt/hyadapt_workload.h"

namespace peloton {
namespace benchmark {
namespace hyadapt {

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

      case EXPERIMENT_TYPE_VERTICAL:
        RunVerticalExperiment();
        break;

      case EXPERIMENT_TYPE_SUBSET:
        RunSubsetExperiment();
        break;

      case EXPERIMENT_TYPE_ADAPT:
        RunAdaptExperiment();
        break;

      case EXPERIMENT_TYPE_WEIGHT:
        RunWeightExperiment();
        break;

      case EXPERIMENT_TYPE_REORG:
        RunReorgExperiment();
        break;

      case EXPERIMENT_TYPE_DISTRIBUTION:
        RunDistributionExperiment();
        break;

      case EXPERIMENT_TYPE_JOIN:
        RunJoinExperiment();
        break;

      case EXPERIMENT_TYPE_INSERT:
        RunInsertExperiment();
        break;

      case EXPERIMENT_TYPE_VERSION:
        RunVersionExperiment();
        break;

      case EXPERIMENT_TYPE_HYRISE:
        RunHyriseExperiment();
        break;

      case EXPERIMENT_TYPE_CONCURRENCY:
        RunConcurrencyExperiment();
        break;

      default:
        LOG_ERROR("Unsupported experiment_type : %d", state.experiment_type);
        break;
    }
  }
}

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::hyadapt::ParseArguments(
      argc, argv, peloton::benchmark::hyadapt::state);

  peloton::benchmark::hyadapt::RunBenchmark();

  return 0;
}
