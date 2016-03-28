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

#include "backend/common/exception.h"
#include "backend/benchmark/logger/logger_configuration.h"

namespace peloton {
namespace benchmark {
namespace logger {

void Usage(FILE* out) {
  fprintf(out,
          "Command line options :  logger <options> \n"
          "   -h --help              :  Print help message \n"
          "   -l --logging-type      :  Logging type \n"
          "   -t --tuple-count       :  Tuple count \n"
          "   -b --backend-count     :  Backend count \n"
          "   -z --column-count      :  # of columns per tuple \n"
          "   -c --check-tuple-count :  Check tuple count \n"
          "   -f --data-file-size    :  Data file size (MB) \n"
          "   -e --experiment_type   :  Experiment Type \n"
          "   -w --wait-timeout      :  Wait timeout (us) \n");
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"logging-type", optional_argument, NULL, 'l'},
    {"tuple-count", optional_argument, NULL, 't'},
    {"backend-count", optional_argument, NULL, 'b'},
    {"column-count", optional_argument, NULL, 'z'},
    {"check-tuple-count", optional_argument, NULL, 'c'},
    {"data-file-size", optional_argument, NULL, 'f'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"wait-timeout", optional_argument, NULL, 'w'},
    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(const configuration& state) {
  std::cout << std::setw(20) << std::left << "logging_type "
            << " : ";

  std::cout << LoggingTypeToString(state.logging_type) << std::endl;
}

static void ValidateColumnCount(const configuration& state) {
  if (state.column_count <= 0) {
    std::cout << "Invalid column_count :: " << state.column_count << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "column_count "
            << " : " << state.column_count << std::endl;
}

static void ValidateTupleCount(const configuration& state) {
  if (state.tuple_count <= 0) {
    std::cout << "Invalid tuple_count :: " << state.tuple_count << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "tuple_count "
            << " : " << state.tuple_count << std::endl;
}

static void ValidateBackendCount(const configuration& state) {
  if (state.backend_count <= 0) {
    std::cout << "Invalid backend_count :: " << state.backend_count
              << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "backend_count "
            << " : " << state.backend_count << std::endl;
}

static void ValidateDataFileSize(const configuration& state) {
  if (state.data_file_size <= 0) {
    std::cout << "Invalid pmem_file_size :: " << state.data_file_size
              << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "data_file_size "
            << " : " << state.data_file_size << std::endl;
}

static void ValidateExperiment(const configuration& state) {
  if (state.experiment_type < 0 || state.experiment_type > 4) {
    std::cout << "Invalid experiment_type :: " << state.experiment_type
              << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "experiment_type "
            << " : " << state.experiment_type << std::endl;
}

static void ValidateWaitTimeout(const configuration& state) {
  if (state.wait_timeout < 0) {
    std::cout << "Invalid wait_timeout :: " << state.wait_timeout << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << std::setw(20) << std::left << "wait_timeout "
            << " : " << state.wait_timeout << std::endl;
}

static void ValidateLogFileDir(configuration& state) {
  struct stat data_stat;

  // Assign log file dir based on logging type
  switch (state.logging_type) {
    // Log file on NVM
    case LOGGING_TYPE_DRAM_NVM:
    case LOGGING_TYPE_NVM_NVM:
    case LOGGING_TYPE_SSD_NVM:
    case LOGGING_TYPE_HDD_NVM: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = NVM_DIR;
      }
    } break;

    // Log file on HDD
    case LOGGING_TYPE_DRAM_HDD:
    case LOGGING_TYPE_NVM_HDD:
    case LOGGING_TYPE_SSD_HDD:
    case LOGGING_TYPE_HDD_HDD: {
      int status = stat(HDD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = HDD_DIR;
      }
    } break;

    // Log file on SSD
    case LOGGING_TYPE_DRAM_SSD:
    case LOGGING_TYPE_NVM_SSD:
    case LOGGING_TYPE_SSD_SSD:
    case LOGGING_TYPE_HDD_SSD: {
      int status = stat(SSD_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = SSD_DIR;
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

  std::cout << std::setw(20) << std::left << "log_file_dir "
            << " : " << state.log_file_dir << std::endl;
}

void ParseArguments(int argc, char* argv[], configuration& state) {
  // Default Values
  state.tuple_count = 10;

  state.logging_type = LOGGING_TYPE_DRAM_NVM;
  state.backend_count = 1;

  state.column_count = 10;

  state.check_tuple_count = false;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;
  state.wait_timeout = 100;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahl:t:b:z:c:f:e:w:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'l':
        state.logging_type = (LoggingType)atoi(optarg);
        break;
      case 't':
        state.tuple_count = atoi(optarg);
        break;
      case 'b':
        state.backend_count = atoi(optarg);
        break;
      case 'z':
        state.column_count = atoi(optarg);
        break;
      case 'c':
        state.check_tuple_count = atoi(optarg);
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

      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }

  // Print configuration
  ValidateLoggingType(state);
  ValidateColumnCount(state);
  ValidateTupleCount(state);
  ValidateBackendCount(state);
  ValidateDataFileSize(state);
  ValidateLogFileDir(state);
  ValidateWaitTimeout(state);
  ValidateExperiment(state);
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
