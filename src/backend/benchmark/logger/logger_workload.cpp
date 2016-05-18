//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_workload.cpp
//
// Identification: src/backend/benchmark/logger/logger_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>
#include <string>
#include <getopt.h>
#include <sys/stat.h>
#include <fts.h>

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/value_factory.h"

#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/logging/log_manager.h"

#include "backend/benchmark/logger/logger_workload.h"
#include "backend/benchmark/logger/logger_loader.h"

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

#include "backend/logging/loggers/wbl_frontend_logger.h"

#include <unistd.h>

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

namespace peloton {
namespace benchmark {
namespace logger {

//===--------------------------------------------------------------------===//
// PREPARE LOG FILE
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// 1. Standby -- Bootstrap
// 2. Recovery -- Optional
// 3. Logging -- Collect data and flush when commit
// 4. Terminate -- Collect any remaining data and flush
// 5. Sleep -- Disconnect backend loggers and frontend logger from manager
//===--------------------------------------------------------------------===//

#define LOGGING_TESTS_DATABASE_OID 20000
#define LOGGING_TESTS_TABLE_OID 10000

std::ofstream out("outputfile.summary");

size_t GetLogFileSize();

static void WriteOutput(double value) {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%d %d %lf %d %d %d %d %d %d %d :: %lf", state.benchmark_type,
           state.logging_type,
           ycsb::state.update_ratio,
           ycsb::state.backend_count,
           ycsb::state.scale_factor,
           ycsb::state.duration,
           state.nvm_latency,
           state.pcommit_latency,
           state.flush_mode,
           state.asynchronous_mode,
           value);

  out << state.benchmark_type << " ";
  out << state.logging_type << " ";
  out << ycsb::state.update_ratio << " ";
  out << ycsb::state.scale_factor << " ";
  out << ycsb::state.backend_count << " ";
  out << ycsb::state.duration << " ";
  out << state.nvm_latency << " ";
  out << state.pcommit_latency << " ";
  out << state.flush_mode << " ";
  out << state.asynchronous_mode << " ";
  out << value << "\n";
  out.flush();
}

std::string GetFilePath(std::string directory_path, std::string file_name) {
  std::string file_path = directory_path;

  // Add a trailing slash to a file path if needed
  if (!file_path.empty() && file_path.back() != '/') file_path += '/';

  file_path += file_name;

  return file_path;
}

void StartLogging(std::thread& thread) {
  auto& log_manager = logging::LogManager::GetInstance();
  if (peloton_logging_mode != LOGGING_TYPE_INVALID) {
    // Launching a thread for logging
    if (!log_manager.IsInLoggingMode()) {
      // Wait for standby mode
      auto local_thread = std::thread(
          &peloton::logging::LogManager::StartStandbyMode, &log_manager);
      thread.swap(local_thread);
      log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_STANDBY,
                                        true);

      // Clean up database tile state before recovery from checkpoint
      log_manager.PrepareRecovery();

      // Do any recovery
      log_manager.StartRecoveryMode();

      // Wait for logging mode
      log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_LOGGING,
                                        true);

      // Done recovery
      log_manager.DoneRecovery();
    }
  }
}

int RemoveDirectory(const char *dir) {
    int ret = 0;
    FTS *ftsp = NULL;
    FTSENT *curr;

    // Cast needed (in C) because fts_open() takes a "char * const *", instead
    // of a "const char * const *", which is only allowed in C++. fts_open()
    // does not modify the argument.
    char *files[] = { (char *) dir, NULL };

    // FTS_NOCHDIR  - Avoid changing cwd, which could cause unexpected behavior
    //                in multithreaded programs
    // FTS_PHYSICAL - Don't follow symlinks. Prevents deletion of files outside
    //                of the specified directory
    // FTS_XDEV     - Don't cross filesystem boundaries
    ftsp = fts_open(files, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
    if (!ftsp) {
        fprintf(stderr, "%s: fts_open failed: %s\n", dir, strerror(errno));
        ret = -1;
        goto finish;
    }

    while ((curr = fts_read(ftsp))) {
        switch (curr->fts_info) {
        case FTS_NS:
        case FTS_DNR:
        case FTS_ERR:
            break;

        case FTS_DC:
        case FTS_DOT:
        case FTS_NSOK:
            // Not reached unless FTS_LOGICAL, FTS_SEEDOT, or FTS_NOSTAT were
            // passed to fts_open()
            break;

        case FTS_D:
            // Do nothing. Need depth-first search, so directories are deleted
            // in FTS_DP
            break;

        case FTS_DP:
        case FTS_F:
        case FTS_SL:
        case FTS_SLNONE:
        case FTS_DEFAULT:
            if (remove(curr->fts_accpath) < 0) {
                fprintf(stderr, "%s: Failed to remove: %s\n",
                        curr->fts_path, strerror(errno));
                ret = -1;
            }
            break;
        }
    }

finish:
    if (ftsp) {
        fts_close(ftsp);
    }

    return ret;
}

void CleanUpLogDirectory() {

  // remove wbl log file if it exists
  std::string wbl_directory_path = state.log_file_dir +
      logging::WriteBehindFrontendLogger::wbl_log_path;

  // remove wal log directory (for wal if it exists)
  // for now hardcode for 1 logger
  std::string wal_directory_path = state.log_file_dir +
      logging::WriteAheadFrontendLogger::wal_directory_path;

  RemoveDirectory(wbl_directory_path.c_str());

  RemoveDirectory(wal_directory_path.c_str());

}

/**
 * @brief writing a simple log file
 */
bool PrepareLogFile() {

  // Clean up log directory
  CleanUpLogDirectory();

  // start a thread for logging
  auto& log_manager = logging::LogManager::GetInstance();
  log_manager.SetLogDirectoryName(state.log_file_dir);
  log_manager.SetLogFileName(state.log_file_dir + "/" + logging::WriteBehindFrontendLogger::wbl_log_path);

  if (log_manager.ContainsFrontendLogger() == true) {
    LOG_ERROR("another logging thread is running now");
    return false;
  }

  // Get an instance of the storage manager to force posix_fallocate
  // to be invoked before we begin benchmarking
  auto& storage_manager = storage::StorageManager::GetInstance();
  auto tmp = storage_manager.Allocate(BACKEND_TYPE_MM, 1024);
  storage_manager.Release(BACKEND_TYPE_MM, tmp);

  // Pick sync commit mode
  switch (state.asynchronous_mode) {
    case ASYNCHRONOUS_TYPE_SYNC:
      log_manager.SetSyncCommit(true);
      break;

    case ASYNCHRONOUS_TYPE_ASYNC:
      log_manager.SetSyncCommit(false);
      break;

    case ASYNCHRONOUS_TYPE_DISABLED:
      // No logging
      peloton_logging_mode = LOGGING_TYPE_INVALID;
      break;

    case ASYNCHRONOUS_TYPE_INVALID:
      throw Exception("Invalid asynchronous mode : " +
                      std::to_string(state.asynchronous_mode));
  }

  std::thread thread;

  // Initializing logging module
  StartLogging(thread);

  // Build the log
  BuildLog();

  // Stop frontend logger if in a valid logging mode
  if (peloton_logging_mode != LOGGING_TYPE_INVALID) {
    //  Wait for the mode transition :: LOGGING -> TERMINATE -> SLEEP
    if (log_manager.EndLogging()) {
      thread.join();
    }
  }

  // Pick metrics based on benchmark type
  double throughput = 0;
  double latency = 0;
  if(state.benchmark_type == BENCHMARK_TYPE_YCSB) {
    throughput = ycsb::state.throughput;
    latency = ycsb::state.latency;
  }
  else if(state.benchmark_type == BENCHMARK_TYPE_TPCC){
    throughput = tpcc::state.throughput;
    latency = tpcc::state.latency;
  }

  // Log the build log time
  if (state.experiment_type == EXPERIMENT_TYPE_THROUGHPUT) {
    WriteOutput(throughput);
  } else if (state.experiment_type == EXPERIMENT_TYPE_LATENCY) {
    WriteOutput(latency);
  }

  return true;
}

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void ResetSystem() {
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.ResetStates();

  // Reset database (only needed for WAL not WBL)
  if(state.benchmark_type == BENCHMARK_TYPE_YCSB) {
    ycsb::CreateYCSBDatabase();
  }
  else if(state.benchmark_type == BENCHMARK_TYPE_TPCC){
    tpcc::CreateTPCCDatabase();
  }

}

/**
 * @brief recover the database and check the tuples
 */
void DoRecovery() {

  //===--------------------------------------------------------------------===//
  // RECOVERY
  //===--------------------------------------------------------------------===//

  // Reset log manager state
  auto& log_manager = peloton::logging::LogManager::GetInstance();
  log_manager.ResetLogStatus();
  log_manager.ResetFrontendLoggers();

  Timer<std::milli> timer;
  std::thread thread;

  timer.Start();

  // Do recovery
  StartLogging(thread);

  // Synchronize and finish recovery
  if(peloton_logging_mode != LOGGING_TYPE_INVALID){
    if (log_manager.EndLogging()) {
      thread.join();
    } else {
      LOG_ERROR("Failed to terminate logging thread");
    }
  }

  timer.Stop();

  // Recovery time (in ms)
  if (state.experiment_type == EXPERIMENT_TYPE_RECOVERY) {
    WriteOutput(timer.GetDuration());
  }

}

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void BuildLog() {

  if(state.benchmark_type == BENCHMARK_TYPE_YCSB) {
    ycsb::CreateYCSBDatabase();

    ycsb::LoadYCSBDatabase();

    ycsb::RunWorkload();
  }
  else if(state.benchmark_type == BENCHMARK_TYPE_TPCC){
    tpcc::CreateTPCCDatabase();

    tpcc::LoadTPCCDatabase();

    tpcc::RunWorkload();
  }

}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
