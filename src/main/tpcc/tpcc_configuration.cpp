//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_configuration.cpp
//
// Identification: src/main/tpcc/tpcc_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpcc <options> \n"
          "   -h --help              :  print help message \n"
          "   -i --index             :  index type: bwtree (default) or btree\n"
          "   -k --scale_factor      :  scale factor \n"
          "   -d --duration          :  execution duration \n"
          "   -p --profile_duration  :  profile duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -w --warehouse_count   :  # of warehouses \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -a --affinity          :  enable client affinity \n"
  );
}

static struct option opts[] = {
    { "index", optional_argument, NULL, 'i' },
    { "scale_factor", optional_argument, NULL, 'k' },
    { "duration", optional_argument, NULL, 'd' },
    { "profile_duration", optional_argument, NULL, 'p' },
    { "backend_count", optional_argument, NULL, 'b' },
    { "warehouse_count", optional_argument, NULL, 'w' },
    { "exp_backoff", no_argument, NULL, 'e' },
    { "affinity", no_argument, NULL, 'a' },
    { NULL, 0, NULL, 0 }
};

void ValidateIndex(const configuration &state) {
  if (state.index != INDEX_TYPE_BTREE && state.index != INDEX_TYPE_BWTREE) {
    LOG_ERROR("Invalid index");
    exit(EXIT_FAILURE);
  }
}

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %lf", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "scale_factor", state.scale_factor);
}

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "duration", state.duration);
}

void ValidateProfileDuration(const configuration &state) {
  if (state.profile_duration <= 0) {
    LOG_ERROR("Invalid profile_duration :: %lf", state.profile_duration);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %lf", "profile_duration", state.profile_duration);
}

void ValidateBackendCount(const configuration &state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "backend_count", state.backend_count);
}

void ValidateWarehouseCount(const configuration &state) {
  if (state.warehouse_count <= 0) {
    LOG_ERROR("Invalid warehouse_count :: %d", state.warehouse_count);
    exit(EXIT_FAILURE);
  }

  LOG_TRACE("%s : %d", "warehouse_count", state.warehouse_count);
}


void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.index = INDEX_TYPE_BWTREE;
  state.scale_factor = 1;
  state.duration = 10;
  state.profile_duration = 1;
  state.backend_count = 2;
  state.warehouse_count = 2;
  state.exp_backoff = false;
  state.affinity = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "heai:k:d:p:b:w:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'i': {
        char *index = optarg;
        if (strcmp(index, "btree") == 0) {
          state.index = INDEX_TYPE_BTREE;
        } else if (strcmp(index, "bwtree") == 0) {
          state.index = INDEX_TYPE_BWTREE;
        } else {
          LOG_ERROR("Unknown index: %s", index);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'k':
        state.scale_factor = atof(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 'p':
        state.profile_duration = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'w':
        state.warehouse_count = atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
    }
  }

  // Static TPCC parameters
  state.item_count = 100000 * state.scale_factor;
  state.districts_per_warehouse = 10;
  state.customers_per_district = 3000 * state.scale_factor;
  state.new_orders_per_district = 900 * state.scale_factor;

  // Print configuration
  ValidateIndex(state);
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateProfileDuration(state);
  ValidateBackendCount(state);
  ValidateWarehouseCount(state);

  LOG_TRACE("%s : %d", "Run client affinity", state.run_affinity);
  LOG_TRACE("%s : %d", "Run exponential backoff", state.run_backoff);
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
