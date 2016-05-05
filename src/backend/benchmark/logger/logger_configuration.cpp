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
          "   -l --logging-type      :  Logging type (THIS IS NOW NUM_LOGGERS)\n"
          "   -f --data-file-size    :  Data file size (MB) \n"
          "   -e --experiment_type   :  Experiment Type \n"
          "   -w --wait-timeout      :  Wait timeout (us) \n"
          "   -h --help              :  Print help message \n"
          "   -k --scale-factor      :  # of tuples \n"
          "   -t --transactions      :  # of transactions \n"
          "   -c --column_count      :  # of columns \n"
          "   -u --write-ratio       :  Fraction of updates \n"
          "   -b --backend-count     :  Backend count \n");
}

static struct option opts[] = {
    {"experiment-type", optional_argument, NULL, 'e'},
    {"scale_factor", optional_argument, NULL, 'k'},
    {"duration", optional_argument, NULL, 'd'},
    {"snapshot_duration", optional_argument, NULL, 's'},
    {"column_count", optional_argument, NULL, 'c'},
    {"update_ratio", optional_argument, NULL, 'u'},
    {"backend_count", optional_argument, NULL, 'b'},
    {"enable_logging", optional_argument, NULL, 'l'},
    {"sync_commit", optional_argument, NULL, 'x'},
    {"wait_time", optional_argument, NULL, 'w'},
    {"file_size", optional_argument, NULL, 'f'},
    {"log_buffer_size", optional_argument, NULL, 'z'},
    {"checkpointer", optional_argument, NULL, 'p'},
    {"flush_freq", optional_argument, NULL, 'q'},

    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(const configuration& state) {
  if (state.logging_type <= LOGGING_TYPE_INVALID) {
    LOG_ERROR("Invalid logging_type :: %d", state.logging_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("Logging_type :: %s", LoggingTypeToString(state.logging_type).c_str());
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
    case LOGGING_TYPE_NVM_WAL:
    case LOGGING_TYPE_NVM_WBL: {
      int status = stat(NVM_DIR, &data_stat);
      if (status == 0 && S_ISDIR(data_stat.st_mode)) {
        state.log_file_dir = NVM_DIR;
      }
    } break;

    // Log file on HDD
    case LOGGING_TYPE_HDD_WAL:
    case LOGGING_TYPE_HDD_WBL: {
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
  state.logging_type = LOGGING_TYPE_NVM_WAL;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_INVALID;
  state.wait_timeout = 200;

  // Default Values
  // Default Values
  ycsb::state.scale_factor = 1;
  ycsb::state.duration = 10;
  ycsb::state.snapshot_duration = 0.1;
  ycsb::state.column_count = 10;
  ycsb::state.update_ratio = 0.5;
  ycsb::state.backend_count = 2;
  ycsb::state.num_loggers = 0;
  ycsb::state.sync_commit = 0;
  ycsb::state.wait_timeout = 0;
  ycsb::state.file_size = 32;
  ycsb::state.log_buffer_size = 32768;
  ycsb::state.checkpointer = 0;
  ycsb::state.flush_freq = 0;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "ahk:d:s:c:u:b:l:x:w:f:z:p:q:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'k':
        ycsb::state.scale_factor = atoi(optarg);
        break;
      case 'd':
        ycsb::state.duration = atof(optarg);
        break;
      case 's':
        ycsb::state.snapshot_duration = atof(optarg);
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
      case 'x':
        ycsb::state.sync_commit = atoi(optarg);
        break;
      case 'l':
        ycsb::state.num_loggers = atoi(optarg);
        break;
      case 'w':
        ycsb::state.wait_timeout = atol(optarg);
        break;
      case 'f':
        ycsb::state.file_size = atoi(optarg);
        break;
      case 'z':
        ycsb::state.log_buffer_size = atoi(optarg);
        break;
      case 'p':
        ycsb::state.checkpointer = atoi(optarg);
        break;
      case 'q':
        ycsb::state.flush_freq = atoi(optarg);
        break;
      case 'h':
        Usage(stderr);
        exit(EXIT_FAILURE);
        break;
      default:
        exit(EXIT_FAILURE);
        break;
    }
  }

  // Print configurations
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
  ycsb::ValidateLogging(ycsb::state);
  ycsb::ValidateDuration(ycsb::state);
  ycsb::ValidateSnapshotDuration(ycsb::state);
  //  ycsb::ValidateFlushFreq(ycsb::state);
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
