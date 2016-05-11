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
          "   -a --asynchronous-mode :  Asynchronous mode \n"
          "   -e --experiment-type   :  Experiment Type \n"
          "   -f --data-file-size    :  Data file size (MB) \n"
          "   -l --logging-type      :  Logging type \n"
          "   -n --nvm-latency       :  NVM latency \n"
          "   -p --pcommit-latency   :  pcommit latency \n"
          "   -v --flush-mode        :  Flush mode \n"
          "   -w --commit-interval   :  Group commit interval \n"
          "   -y --benchmark-type    :  Benchmark type \n"
  );
}

static struct option opts[] = {
    {"asynchronous_mode", optional_argument, NULL, 'a'},
    {"experiment-type", optional_argument, NULL, 'e'},
    {"data-file-size", optional_argument, NULL, 'f'},
    {"logging-type", optional_argument, NULL, 'l'},
    {"nvm-latency", optional_argument, NULL, 'n'},
    {"pcommit-latency", optional_argument, NULL, 'p'},
    {"skew", optional_argument, NULL, 's'},
    {"flush-mode", optional_argument, NULL, 'v'},
    {"commit-interval", optional_argument, NULL, 'w'},
    {"benchmark-type", optional_argument, NULL, 'y'},
    {NULL, 0, NULL, 0}};

static void ValidateLoggingType(const configuration& state) {
  if (state.logging_type <= LOGGING_TYPE_INVALID) {
    LOG_ERROR("Invalid logging_type :: %d", state.logging_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("Logging_type :: %s", LoggingTypeToString(state.logging_type).c_str());
}

std::string BenchmarkTypeToString(BenchmarkType type) {
  switch (type) {
    case BENCHMARK_TYPE_INVALID:
      return "INVALID";

    case BENCHMARK_TYPE_YCSB:
      return "YCSB";
    case BENCHMARK_TYPE_TPCC:
      return "TPCC";

    default:
      LOG_ERROR("Invalid benchmark_type :: %d", type);
      exit(EXIT_FAILURE);
  }
  return "INVALID";
}

std::string ExperimentTypeToString(ExperimentType type) {
  switch (type) {
    case EXPERIMENT_TYPE_INVALID:
      return "INVALID";

    case EXPERIMENT_TYPE_THROUGHPUT:
      return "THROUGHPUT";
    case EXPERIMENT_TYPE_RECOVERY:
      return "RECOVERY";
    case EXPERIMENT_TYPE_STORAGE:
      return "STORAGE";
    case EXPERIMENT_TYPE_LATENCY:
      return "LATENCY";

    default:
      LOG_ERROR("Invalid experiment_type :: %d", type);
      exit(EXIT_FAILURE);
  }

  return "INVALID";
}

std::string AsynchronousTypeToString(AsynchronousType type) {
  switch (type) {
    case ASYNCHRONOUS_TYPE_INVALID:
      return "INVALID";

    case ASYNCHRONOUS_TYPE_SYNC:
      return "SYNC";
    case ASYNCHRONOUS_TYPE_ASYNC:
      return "ASYNC";
    case ASYNCHRONOUS_TYPE_DISABLED:
      return "DISABLED";

    default:
      LOG_ERROR("Invalid asynchronous_mode :: %d", type);
      exit(EXIT_FAILURE);
  }

  return "INVALID";
}


static void ValidateBenchmarkType(
    const configuration& state) {
  if (state.benchmark_type <= 0 || state.benchmark_type > 3) {
    LOG_ERROR("Invalid benchmark_type :: %d", state.benchmark_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %s", "benchmark_type", BenchmarkTypeToString(state.benchmark_type).c_str());
}

static void ValidateDataFileSize(
    const configuration& state) {
  if (state.data_file_size <= 0) {
    LOG_ERROR("Invalid data_file_size :: %lu", state.data_file_size);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("data_file_size :: %lu", state.data_file_size);
}

static void ValidateExperimentType(
    const configuration& state) {
  if (state.experiment_type < 0 || state.experiment_type > 4) {
    LOG_ERROR("Invalid experiment_type :: %d", state.experiment_type);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %s", "experiment_type", ExperimentTypeToString(state.experiment_type).c_str());
}

static void ValidateWaitTimeout(const configuration& state) {
  if (state.wait_timeout < 0) {
    LOG_ERROR("Invalid wait_timeout :: %d", state.wait_timeout);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("wait_timeout :: %d", state.wait_timeout);
}

static void ValidateFlushMode(
    const configuration& state) {
  if (state.flush_mode <= 0 || state.flush_mode >= 3) {
    LOG_ERROR("Invalid flush_mode :: %d", state.flush_mode);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("flush_mode :: %d", state.flush_mode);
}

static void ValidateAsynchronousMode(
    const configuration& state) {
  if (state.asynchronous_mode <= ASYNCHRONOUS_TYPE_INVALID ||
      state.asynchronous_mode > ASYNCHRONOUS_TYPE_DISABLED) {
    LOG_ERROR("Invalid asynchronous_mode :: %d", state.asynchronous_mode);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %s", "asynchronous_mode", AsynchronousTypeToString(state.asynchronous_mode).c_str());
}

static void ValidateNVMLatency(
    const configuration& state) {
  if (state.nvm_latency < 0) {
    LOG_ERROR("Invalid nvm_latency :: %d", state.nvm_latency);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("nvm_latency :: %d", state.nvm_latency);
}

static void ValidatePCOMMITLatency(
    const configuration& state) {
  if (state.pcommit_latency < 0) {
    LOG_ERROR("Invalid pcommit_latency :: %d", state.pcommit_latency);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("pcommit_latency :: %d", state.pcommit_latency);
}

static void ValidateLogFileDir(
    configuration& state) {
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

  // Default Logger Values
  state.logging_type = LOGGING_TYPE_NVM_WAL;

  state.log_file_dir = TMP_DIR;
  state.data_file_size = 512;

  state.experiment_type = EXPERIMENT_TYPE_THROUGHPUT;
  state.wait_timeout = 200;
  state.benchmark_type = BENCHMARK_TYPE_YCSB;
  state.flush_mode = 2;
  state.nvm_latency = 0;
  state.pcommit_latency = 0;
  state.asynchronous_mode = ASYNCHRONOUS_TYPE_SYNC;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "a:e:f:hl:n:p:v:w:y:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'a':
        state.asynchronous_mode = (AsynchronousType) atoi(optarg);
        break;
      case 'e':
        state.experiment_type = (ExperimentType)atoi(optarg);
        break;
      case 'f':
        state.data_file_size = atoi(optarg);
        break;
      case 'l':
        state.logging_type = (LoggingType)atoi(optarg);
        break;
      case 'n':
        state.nvm_latency = atoi(optarg);
        break;
      case 'p':
        state.pcommit_latency = atoi(optarg);
        break;
      case 'v':
        state.flush_mode = atoi(optarg);
        break;
      case 'w':
        state.wait_timeout = atoi(optarg);
        break;
      case 'y':
        state.benchmark_type = (BenchmarkType)atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        ycsb::Usage(stderr);
        exit(EXIT_FAILURE);
        break;

      default:
        // Ignore unknown options
        break;
    }
  }

  // Print configurations
  ValidateAsynchronousMode(state);
  ValidateLoggingType(state);
  ValidateDataFileSize(state);
  ValidateLogFileDir(state);
  ValidateWaitTimeout(state);
  ValidateExperimentType(state);
  ValidateBenchmarkType(state);
  ValidateFlushMode(state);
  ValidateNVMLatency(state);
  ValidatePCOMMITLatency(state);

  // Send the arguments to YCSB
  ycsb::ParseArguments(argc, argv, ycsb::state);

}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
