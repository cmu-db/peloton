//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb.cpp
//
// Identification: benchmark/ycsb/ycsb.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>

#include "backend/benchmark/ycsb/ycsb.h"
#include "backend/benchmark/ycsb/configuration.h"
#include "backend/benchmark/ycsb/workload.h"

namespace peloton {
namespace benchmark {
namespace ycsb{

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
    case OPERATOR_TYPE_READ:
      RunRead();
      break;

    case OPERATOR_TYPE_SCAN:
      RunScan();
      break;

    case OPERATOR_TYPE_INSERT:
      RunInsert();
      break;

    case OPERATOR_TYPE_UPDATE:
      RunUpdate();
      break;

    case OPERATOR_TYPE_DELETE:
      RunDelete();
      break;

    case OPERATOR_TYPE_READ_MODIFY_WRITE:
      RunReadModifyWrite();
      break;

    default:
      std::cout << "Unsupported test type : " << state.operator_type << "\n";
      break;
  }

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
