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

namespace peloton {
namespace benchmark {
namespace logger {

void Usage() {
  LOG_ERROR(
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
  LOG_INFO("Invalid logging_type :: %s", LoggingTypeToString(state.logging_type).c_str());
}

static void ValidateColumnCount(const configuration& state) {
  if (state.column_count <= 0) {
    LOG_ERROR("Invalid column_count :: %ld", state.column_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("column_count :: %ld", state.column_count);
}

static void ValidateTupleCount(const configuration& state) {
  if (state.tuple_count <= 0) {
    LOG_ERROR("Invalid tuple_count :: %d", state.tuple_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("tuple_count :: %d", state.tuple_count);
}

static void ValidateBackendCount(const configuration& state) {
  if (state.backend_count <= 0) {
    LOG_ERROR("Invalid backend_count :: %d", state.backend_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("backend_count :: %d", state.backend_count);
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

  LOG_INFO("log_file_dir :: %s", state.log_file_dir.c_str());
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
        Usage();
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage();
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
