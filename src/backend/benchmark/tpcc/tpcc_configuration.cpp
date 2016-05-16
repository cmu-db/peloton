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
          "   -k --scale_factor      :  scale factor \n"
  );
}

static struct option opts[] = {
    { "backend_count", optional_argument, NULL, 'b'},
    { "duration", optional_argument, NULL, 'd' },
    { "scale_factor", optional_argument, NULL, 'k' },
    {NULL, 0, NULL, 0}
};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
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
  state.scale_factor = 1;
  state.duration = 1000;
  state.backend_count = 2;

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
        state.scale_factor = atoi(optarg);
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
  state.warehouse_count = 2;                   // 10
  state.item_count = 10000;                    // 100000
  state.districts_per_warehouse = 2;           // 10
  state.customers_per_district = 300;          // 3000
  state.new_orders_per_district = 90;          // 900

  // Print configuration
  ValidateBackendCount(state);
  ValidateScaleFactor(state);
  ValidateDuration(state);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
