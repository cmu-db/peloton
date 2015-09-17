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

#include "backend/benchmark/hyadapt/hyadapt.h"
#include "backend/benchmark/hyadapt/configuration.h"
#include "backend/benchmark/hyadapt/workload.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

configuration state;

// Main Entry Point
void RunBenchmark(){

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

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {

  peloton::benchmark::hyadapt::ParseArguments(argc, argv,
                                               peloton::benchmark::hyadapt::state);

  peloton::benchmark::hyadapt::RunBenchmark();

  return 0;
}
