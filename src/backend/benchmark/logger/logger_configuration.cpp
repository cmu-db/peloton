//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: benchmark/logger/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iomanip>
#include <algorithm>
#include <sys/stat.h>

#include "backend/common/exception.h"
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
          "   -u --write_ratio       :  Fraction of updates \n"
          "   -b --backend-count     :  Backend count \n"
  );
}

static struct option opts[] = {
    {"logging-type", optional_argument, NULL, 'l'},
    {"data-file-size", optional_argument, NULL, 'f'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"wait-timeout", optional_argument, NULL, 'w'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"transactions", optional_argument, NULL, 't'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(
    const configuration& state) {
  std::cout << std::setw(20) << std::left << "logging_type "
      << " : ";

  std::cout << LoggingTypeToString(state.logging_type) << std::endl;
}

static void ValidateDataFileSize(
    const configuration& state) {
  if (state.data_file_size <= 0) {
    std::cout << "Invalid pmem_file_size :: " << state.data_file_size
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "data_file_size "
      << " : " << state.data_file_size << std::endl;
}

static void ValidateExperiment(
    const configuration& state) {
  if (state.experiment_type < 0 || state.experiment_type > 4) {
    std::cout << "Invalid experiment_type :: " << state.experiment_type
        << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "experiment_type "
      << " : " << state.experiment_type << std::endl;
}

static void ValidateWaitTimeout(
    const configuration& state) {
  if (state.wait_timeout < 0) {
    std::cout << "Invalid wait_timeout :: " << state.wait_timeout << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "wait_timeout "
      << " : " << state.wait_timeout << std::endl;
}

static void ValidateLogFileDir(
    configuration& state) {
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
      }
      else {
        throw Exception("Could not find temp directory : " + std::string(TMP_DIR));
      }
    } break;
  }

  std::cout << std::setw(20) << std::left << "log_file_dir "
      << " : " << state.log_file_dir << std::endl;
}

void ParseArguments(int argc, char* argv[], configuration &state) {
  // Default Values
  state.logging_type = LOGGING_TYPE_DRAM_NVM;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;
  state.wait_timeout = 100;

  // Default Values
  ycsb::state.scale_factor = 1;
  ycsb::state.transactions = 10000;
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
        ycsb::state.transactions = atoi(optarg);
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
        ycsb::Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
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

}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
