//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.cpp
//
// Identification: src/backend/benchmark/logger/logger_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iomanip>
#include <algorithm>
#include <sys/stat.h>

#undef NDEBUG

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/benchmark/logger/logger_configuration.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"

namespace peloton {
namespace benchmark {
namespace logger {

void Usage(FILE* out) {
  fprintf(out,
          "Command line options :  logger <options> \n"
          "   -h --help              :  Print help message \n"
          "   -l --logging-type      :  Logging type \n"
          "   -f --data-file-size    :  Data file size (MB) \n"
          "   -e --experiment_type   :  Experiment Type \n"
          "   -w --wait-timeout      :  Wait timeout (us) \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -t --transactions      :  # of transactions \n"
          "   -c --column_count      :  # of columns \n"
          "   -u --write-ratio       :  Fraction of updates \n"
          "   -b --backend-count     :  Backend count \n"
  );
}

static struct option opts[] = {
    {"logging-type", optional_argument, NULL, 'l'},
    {"data-file-size", optional_argument, NULL, 'f'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"wait-timeout", optional_argument, NULL, 'w'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"transaction_count", optional_argument, NULL, 't'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"backend_count", optional_argument, NULL, 'b'},
    {NULL, 0, NULL, 0}};


static void ValidateLoggingType(const configuration& state) {
  LOG_INFO("Invalid logging_type :: %s", LoggingTypeToString(state.logging_type).c_str());
}

static void ValidateDataFileSize(const configuration& state) {
  if (state.data_file_size <= 0) {
    LOG_ERROR("Invalid data_file_size :: %lu", state.data_file_size);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("data_file_size :: %lu", state.data_file_size);
}

static void ValidateExperiment(const configuration& state) {
  if (state.experiment_type < 0 || state.experiment_type > 4) {
    LOG_ERROR("Invalid experiment_type :: %d", state.experiment_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("experiment_type :: %d", state.experiment_type);
}

static void ValidateWaitTimeout(const configuration& state) {
  if (state.wait_timeout < 0) {
    LOG_ERROR("Invalid wait_timeout :: %lu", state.wait_timeout);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("wait_timeout :: %lu", state.wait_timeout);
}

static void ValidateLogFileDir(configuration& state) {
  struct stat data_stat;

  // Assign log file dir based on logging type
  switch (state.logging_type) {
    // Log file on NVM
    case LOGGING_TYPE_DRAM_NVM:
    case LOGGING_TYPE_NVM_NVM:
    case LOGGING_TYPE_HDD_NVM: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = NVM_DIR;
      }
    } break;

    // Log file on HDD
    case LOGGING_TYPE_DRAM_HDD:
    case LOGGING_TYPE_NVM_HDD:
    case LOGGING_TYPE_HDD_HDD: {
      int status = stat(HDD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = HDD_DIR;
      }
    } break;

    case LOGGING_TYPE_INVALID:
    default: {
      int status = stat(TMP_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = TMP_DIR;
      } else {
        throw Exception("Could not find temp directory : " +
                        std::string(TMP_DIR));
      }
    } break;
  }

  LOG_INFO("log_file_dir :: %s", state.log_file_dir.c_str());
}

void ParseArguments(int argc, char* argv[], configuration& state) {
  // Default Values
  state.logging_type = LOGGING_TYPE_DRAM_NVM;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;
  state.wait_timeout = 200;

  // Default Values
  ycsb::state.scale_factor = 1;
  ycsb::state.transaction_count = 10000;
  ycsb::state.column_count = 10;
  ycsb::state.update_ratio = 0.5;
  ycsb::state.backend_count = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahl:f:e:w:k:t:c:u:b:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'l':
        state.logging_type = (LoggingType)atoi(optarg);
        break;
      case 'f':
        state.data_file_size = atoi(optarg);
        break;
      case 'e':
        state.experiment_type = (ExperimentType)atoi(optarg);
        break;
      case 'w':
        state.wait_timeout = atoi(optarg);
        break;

        // YCSB
      case 'k':
        ycsb::state.scale_factor = atoi(optarg);
        break;
      case 't':
        ycsb::state.transaction_count = atoi(optarg);
        break;
      case 'c':
        ycsb::state.column_count = atoi(optarg);
        break;
      case 'u':
        ycsb::state.update_ratio = atof(optarg);
        break;
      case 'b':
        ycsb::state.backend_count = atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        break;

      default:
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configuration
  ValidateLoggingType(state);
  ValidateDataFileSize(state);
  ValidateLogFileDir(state);
  ValidateWaitTimeout(state);
  ValidateExperiment(state);

  // Print configuration
  ycsb::ValidateScaleFactor(ycsb::state);
  ycsb::ValidateColumnCount(ycsb::state);
  ycsb::ValidateUpdateRatio(ycsb::state);
  ycsb::ValidateBackendCount(ycsb::state);
  ycsb::ValidateTransactionCount(ycsb::state);

}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
