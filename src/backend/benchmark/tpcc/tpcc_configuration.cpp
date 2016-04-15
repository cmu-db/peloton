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
          "   -k --scale_factor      :  scale factor \n"
          "   -w --warehouse_count   :  # of warehouses \n"
          "   -b --backend_count     :  # of backends \n"
          "   -t --transaction_count :  # of transactions \n"
          );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale_factor", optional_argument, NULL, 'k'},
    {"warehouse_count", optional_argument, NULL, 'w'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"transaction_count", optional_argument, NULL, 't'},
    {NULL, 0, NULL, 0}};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "backend_count", state.backend_count);
}

void ValidateTransactionCount(const configuration &state) {
  if (state.transaction_count <= 0) {
    LOG_ERROR("Invalid transaction_count :: %lu", state.transaction_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lu", "transaction_count", state.transaction_count);
}

void ValidateWarehouseCount(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "warehouse_count", state.warehouse_count);
}

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.backend_count = 1;
  state.warehouse_count = 1;
  state.transaction_count = 100;
  state.scale_factor = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ah:k:w:b:t:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;

      case 'w':
        state.warehouse_count = atoi(optarg);
        break;

      case 'b':
        state.backend_count = atoi(optarg);
        break;

      case 't':
        state.transaction_count = atoi(optarg);
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

  // Static parameters
  state.item_count = 10 * state.scale_factor;
  state.districts_per_warehouse = 2;
  state.customers_per_district = 30;
  state.new_orders_per_district = 9;

  // Print configuration
  ValidateBackendCount(state);
  ValidateWarehouseCount(state);
  ValidateScaleFactor(state);
  ValidateTransactionCount(state);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
