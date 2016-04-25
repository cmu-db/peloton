//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/ycsb/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#undef NDEBUG

#include <iomanip>
#include <algorithm>

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/common/logger.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out, "Command line options : ycsb <options> \n"
               "   -h --help              :  Print help message \n"
               "   -k --scale_factor      :  # of tuples \n"
               "   -d --duration          :  execution duration \n"
               "   -s --snapshot_duration :  snapshot duration \n"
               "   -c --column_count      :  # of columns \n"
               "   -u --write_ratio       :  Fraction of updates \n"
               "   -b --backend_count     :  # of backends \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
  { "scale_factor", optional_argument, NULL, 'k' },
  { "duration", optional_argument, NULL, 'd' },
  { "snapshot_duration", optional_argument, NULL, 's' },
  { "column_count", optional_argument, NULL, 'c' },
  { "update_ratio", optional_argument, NULL, 'u' },
  { "backend_count", optional_argument, NULL, 'b' }, { NULL, 0, NULL, 0 }
};

void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
}

void ValidateColumnCount(const configuration &state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "column_count", state.column_count);
}

void ValidateUpdateRatio(const configuration &state) {
  if (state.update_ratio < 0 || state.update_ratio > 1) {
    LOG_ERROR("Invalid update_ratio :: %lf", state.update_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lf", "update_ratio", state.update_ratio);
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

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.scale_factor = 1;
  state.duration = 10;
  state.snapshot_duration = 0.1;
  state.column_count = 10;
  state.update_ratio = 0.5;
  state.backend_count = 2;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahk:d:s:c:u:b:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'd':
        state.duration = atof(optarg);
        break;
      case 's':
        state.snapshot_duration = atof(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
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

  // Print configuration
  ValidateScaleFactor(state);
  ValidateColumnCount(state);
  ValidateUpdateRatio(state);
  ValidateBackendCount(state);
  ValidateDuration(state);
  ValidateSnapshotDuration(state);

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
