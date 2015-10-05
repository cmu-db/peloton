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
  for(auto column_id = 1; column_id <= ATTRIBUTE_COUNT; column_id++)
    hyadapt_column_ids.push_back(column_id);
  std::random_shuffle(hyadapt_column_ids.begin(), hyadapt_column_ids.end());

  if(peloton_layout == LAYOUT_ROW && peloton_projectivity < 0.5){
    // Set query processing engine
    state.scale_factor /= QUERY_ENGINE_SCALE;
    state.tuples_per_tilegroup *= QUERY_ENGINE_SCALE;
  }

  // Single run
  if(state.experiment_type == EXPERIMENT_TYPE_INVALID) {
    std::unique_ptr<storage::DataTable>table(CreateAndLoadTable(peloton_layout));

    switch(state.operator_type) {
      case OPERATOR_TYPE_DIRECT:
        RunDirectTest(table.get());
        break;

      case OPERATOR_TYPE_AGGREGATE:
        RunAggregateTest(table.get());
        break;

      case OPERATOR_TYPE_ARITHMETIC:
        RunArithmeticTest(table.get());
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
