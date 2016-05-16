//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/tpcc/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>

#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpcc <options> \n"
          "   -h --help              :  Print help message \n"
          "   -b --backend_count     :  # of backends \n"
          "   -d --duration          :  execution duration \n"
          "   -k --warehouse_count   :  warehouse count \n"
  );
}

static struct option opts[] = {
    { "backend_count", optional_argument, NULL, 'b'},
    { "duration", optional_argument, NULL, 'd' },
    { "warehouse_count", optional_argument, NULL, 'k' },
    {NULL, 0, NULL, 0}
};

void ValidateWarehouseCount(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "warehouse_count", state.warehouse_count);
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %d", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "duration", state.duration);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "backend_count", state.backend_count);
}

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.duration = 1000;
  state.backend_count = 2;
  state.warehouse_count = 2;  // 10

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ah:b:d:k:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'd':
        state.duration = atoi(optarg);
        break;
      case 'k':
        state.warehouse_count = atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
    }
  }

  // Static TPCC parameters
  state.item_count = 10000;                    // 100000
  state.districts_per_warehouse = 2;           // 10
  state.customers_per_district = 300;          // 3000
  state.new_orders_per_district = 90;          // 900

  // Print configuration
  ValidateBackendCount(state);
  ValidateWarehouseCount(state);
  ValidateDuration(state);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
