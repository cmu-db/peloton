//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tpcc.cpp
//
// Identification: benchmark/tpcc/tpcc.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/benchmark/tpcc/tpcc.h"
#include "backend/benchmark/tpcc/configuration.h"
#include "backend/benchmark/tpcc/workload.h"

namespace peloton {
namespace benchmark {
namespace tpcc{

configuration state;

// Main Entry Point
void RunBenchmark(){

  // Initialize settings
  peloton_layout = state.layout;

  // Single run
  CreateAndLoadTable((LayoutType) peloton_layout);

  // Initialize random number generator
  srand(0);

  switch(state.operator_type) {
    case OPERATOR_TYPE_NEW_ORDER:
      RunNewOrder();
      break;

    default:
      std::cout << "Unsupported test type : " << state.operator_type << "\n";
      break;
  }

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {

  peloton::benchmark::tpcc::ParseArguments(argc, argv,
                                           peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}
