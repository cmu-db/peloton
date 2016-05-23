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
#include <string.h>

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
          "   -d --duration          :  execution duration \n"
          "   -s --snapshot_duration :  snapshot duration \n"
          "   -b --backend_count     :  # of backends \n"
          "   -w --warehouse_count   :  # of warehouses \n"
          "   -e --exp_backoff       :  enable exponential backoff \n"
          "   -p --protocol          :  choose protocol, default OCC\n"
          "                             protocol could be occ, pcc, ssi, sread, ewrite, occrb, and to\n"
          "   -g --gc_protocol       :  choose gc protocol, default OFF\n"
          "                             gc protocol could be off, co, va"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
  { "scale_factor", optional_argument, NULL, 'k' },
  { "duration", optional_argument, NULL, 'd' },
  { "snapshot_duration", optional_argument, NULL, 's' },
  { "backend_count", optional_argument, NULL, 'b'},
  { "warehouse_count", optional_argument, NULL, 'w' },
  { "exp_backoff", no_argument, NULL, 'e'},
  { "protocol", optional_argument, NULL, 'p'},
  { "gc_protocol", optional_argument, NULL, 'g'},
  { NULL, 0, NULL, 0 }
};

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

void ValidateDuration(const configuration &state) {
  if (state.duration <= 0) {
    LOG_ERROR("Invalid duration :: %lf", state.duration);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "execution duration", state.duration);
}

void ValidateSnapshotDuration(const configuration &state) {
  if (state.snapshot_duration <= 0) {
    LOG_ERROR("Invalid snapshot_duration :: %lf", state.snapshot_duration);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "snapshot_duration", state.snapshot_duration);
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
  state.scale_factor = 1;
  state.duration = 10;
  state.snapshot_duration = 0.1;
  state.backend_count = 1;
  state.warehouse_count = 1;
  state.run_backoff = false;
  state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
  state.gc_protocol = GC_TYPE_OFF;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ah:k:w:d:s:b:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'w':
        state.warehouse_count = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 's':
        state.snapshot_duration = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'e':
        state.run_backoff = true;
        break;
      case 'p': {
        char *protocol = optarg;
        if (strcmp(protocol, "occ") == 0) {
          state.protocol = CONCURRENCY_TYPE_OPTIMISTIC;
        } else if (strcmp(protocol, "pcc") == 0) {
          state.protocol = CONCURRENCY_TYPE_PESSIMISTIC;
        } else if (strcmp(protocol, "ssi") == 0) {
          state.protocol = CONCURRENCY_TYPE_SSI;
        } else if (strcmp(protocol, "to") == 0) {
          state.protocol = CONCURRENCY_TYPE_TO;
        } else if (strcmp(protocol, "ewrite") == 0) {
          state.protocol = CONCURRENCY_TYPE_EAGER_WRITE;
        } else if (strcmp(protocol, "occrb") == 0) {
          state.protocol = CONCURRENCY_TYPE_OCC_RB;
        } else if (strcmp(protocol, "sread") == 0) {
          state.protocol = CONCURRENCY_TYPE_SPECULATIVE_READ;
        } else {
          fprintf(stderr, "\nUnknown protocol: %s\n", protocol);
          exit(EXIT_FAILURE);
        }
        break;
      }
      case 'g': {
        char *gc_protocol = optarg;
        if (strcmp(gc_protocol, "off") == 0) {
          state.gc_protocol = GC_TYPE_OFF;
        }else if (strcmp(gc_protocol, "va") == 0) {
          state.gc_protocol = GC_TYPE_VACUUM;
        }else if (strcmp(gc_protocol, "co") == 0) {
          state.gc_protocol = GC_TYPE_CO;
        }else {
          fprintf(stderr, "\nUnknown gc protocol: %s\n", gc_protocol);
          exit(EXIT_FAILURE);
        }
        break;
      }
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
  ValidateScaleFactor(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);
  ValidateWarehouseCount(state);
  ValidateBackendCount(state);
  
  LOG_INFO("%s : %d", "Run exponential backoff", state.run_backoff);

}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton