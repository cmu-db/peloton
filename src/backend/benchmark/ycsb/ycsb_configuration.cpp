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

// #undef NDEBUG

#include <iomanip>
#include <algorithm>

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/common/logger.h"

#undef NDEBUG

namespace peloton {
namespace benchmark {
namespace ycsb {

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -b --backend-count     :  # of backends \n"
          "   -c --column-count      :  # of columns \n"
          "   -d --duration          :  execution duration \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -s --skew              :  Skew factor \n"
          "   -u --update-ratio      :  Fraction of updates \n"
          );
}

static struct option opts[] = {
    {"backend-count", optional_argument, NULL, 'b'},
    {"column-count", optional_argument, NULL, 'c'},
    {"duration", optional_argument, NULL, 'd'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"skew", optional_argument, NULL, 's'},
    {"update-ratio", optional_argument, NULL, 'u'},
    {NULL, 0, NULL, 0}};

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

void ValidateSkewFactor(const configuration &state) {
  if (state.skew_factor <= 0 || state.skew_factor >= 3) {
    std::cout << "Invalid skew_factor :: " << state.skew_factor
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "skew_factor "
      << " : " << state.skew_factor << std::endl;
}

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.scale_factor = 1;
  state.duration = 1000;
  state.column_count = 10;
  state.update_ratio = 0.5;
  state.backend_count = 2;
  state.skew_factor = SKEW_FACTOR_LOW;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hb:c:d:k:s:u:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'c':
        state.column_count = atoi(optarg);
        break;
      case 'd':
        state.duration = atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 's':
        state.skew_factor = (SkewFactor)atoi(optarg);
        break;
      case 'u':
        state.update_ratio = atof(optarg);
        break;

      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
      default:
        // Ignore unknown options
        break;
    }
  }

  // Print configuration
  ValidateScaleFactor(state);
  ValidateColumnCount(state);
  ValidateUpdateRatio(state);
  ValidateBackendCount(state);
  ValidateDuration(state);
  ValidateSkewFactor(state);

}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
