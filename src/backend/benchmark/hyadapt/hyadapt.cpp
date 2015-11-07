//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hyadapt.cpp
//
// Identification: benchmark/hyadapt/hyadapt.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/benchmark/hyadapt/hyadapt.h"
#include "backend/benchmark/hyadapt/configuration.h"
#include "backend/benchmark/hyadapt/workload.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

configuration state;

// Main Entry Point
void RunBenchmark(){

  // Initialize settings
  peloton_layout = state.layout;
  peloton_projectivity = state.projectivity;

  // Generate sequence
  GenerateSequence(state.column_count);

  // Single run
  if(state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    CreateAndLoadTable((LayoutType) peloton_layout);

    switch(state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        RunDirectTest();
        break;

      case OPERATOR_TYPE_AGGREGATE:
        RunAggregateTest();
        break;

      case OPERATOR_TYPE_ARITHMETIC:
        RunArithmeticTest();
        break;

      default:
        std::cout << "Unsupported test type : " << state.operator_type << "\n";
        break;
    }

  }
  // Experiment
  else {

    switch(state.experiment_type) {
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

      case EXPERIMENT_TYPE_THETA:
        RunThetaExperiment();
        break;

      case EXPERIMENT_TYPE_WEIGHT:
        RunWeightExperiment();
        break;

      default:
        std::cout << "Unsupported experiment type : " << state.experiment_type << "\n";
        break;
    }

  }



}

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {

  peloton::benchmark::hyadapt::ParseArguments(argc, argv,
                                              peloton::benchmark::hyadapt::state);

  peloton::benchmark::hyadapt::RunBenchmark();

  return 0;
}
