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
  fprintf(out,
          "Command line options : ycsb <options> \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -t --transactions      :  # of transactions \n"
          "   -c --column_count      :  # of columns \n"
          "   -u --write_ratio       :  Fraction of updates \n"
          "   -b --backend_count     :  # of backends \n"
          "   -l --enable_logging    :  enable_logging (0 or 1) \n"
          "   -s --sync_commit       :  enable synchronous commit (0 or 1) \n");
  // TODO add wait_time, file_size, log_buffer_size
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"scale-factor", optional_argument, NULL, 'k'},
    {"transactions", optional_argument, NULL, 't'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"enable_logging", optional_argument, NULL, 'l'},
    {"sync_commit", optional_argument, NULL, 's'},
    {"wait_time", optional_argument, NULL, 'w'},
    {"file_size", optional_argument, NULL, 'f'},
    {"log_buffer_size", optional_argument, NULL, 'z'},
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

void ValidateTransactionCount(const configuration &state) {
  if (state.transaction_count <= 0) {
    LOG_ERROR("Invalid transaction_count :: %lu", state.transaction_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lu", "transaction_count", state.transaction_count);
}

void ValidateLogging(const configuration &state) {
  // TODO validate that sync_commit is enabled only when logging is enabled
  LOG_INFO("%s : %d", "logging_enabled", state.logging_enabled);
  LOG_INFO("%s : %d", "synchronous_commit", state.sync_commit);
  LOG_INFO("%s : %d", "wait_time", (int)state.wait_timeout);
}

void ParseArguments(int argc, char *argv[], configuration &state) {
  // Default Values
  state.scale_factor = 1;
  state.transaction_count = 10000;
  state.column_count = 10;
  state.update_ratio = 0.5;
  state.backend_count = 2;
  state.logging_enabled = 0;
  state.sync_commit = 0;
  state.wait_timeout = 0;
  state.file_size = 32;
  state.log_buffer_size = 32768;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahk:t:c:u:b:l:s:w:f:z:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 't':
        state.transaction_count = atoi(optarg);
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
      case 's':
        state.sync_commit = atoi(optarg);
        break;
      case 'l':
        state.logging_enabled = atoi(optarg);
        break;
      case 'w':
        state.wait_timeout = atol(optarg);
        break;
      case 'f':
        state.file_size = atoi(optarg);
        break;
      case 'z':
        state.log_buffer_size = atoi(optarg);
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
  ValidateTransactionCount(state);
  ValidateLogging(state);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
