//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb.cpp
//
// Identification: src/backend/benchmark/ycsb/ycsb.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <fstream>
#include <thread>

#include "backend/common/logger.h"
#include "backend/common/types.h"

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

#include "backend/logging/log_manager.h"

extern LoggingType peloton_logging_mode;

extern int64_t peloton_wait_timeout;

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat) {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d %d %d %d %d :: %lf tps", state.update_ratio,
           state.scale_factor, state.column_count, state.logging_enabled,
           state.sync_commit, state.file_size, state.checkpointer, stat);

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << " ";
  out << state.logging_enabled << " ";
  out << state.sync_commit << " ";
  out << state.wait_timeout << " ";
  out << state.file_size << " ";
  out << state.checkpointer << " ";
  out << stat << "\n";
  out.flush();
}

inline void YCSBBootstrapLogger() {
  if (state.logging_enabled <= 0) return;
  peloton_logging_mode = LOGGING_TYPE_NVM_WAL;

  auto& log_manager = peloton::logging::LogManager::GetInstance();
  auto& checkpoint_manager = peloton::logging::CheckpointManager::GetInstance();

  if (state.checkpointer != 0) {
    peloton_checkpoint_mode = CHECKPOINT_TYPE_NORMAL;

    // launch checkpoint thread
    if (!checkpoint_manager.IsInCheckpointingMode()) {
      // Wait for standby mode
      std::thread(&peloton::logging::CheckpointManager::StartStandbyMode,
                  &checkpoint_manager).detach();
      checkpoint_manager.WaitForModeTransition(
          peloton::CHECKPOINT_STATUS_STANDBY, true);

      // Clean up table tile state before recovery from checkpoint
      log_manager.PrepareRecovery();

      // Do any recovery
      checkpoint_manager.StartRecoveryMode();

      // Wait for standby mode
      checkpoint_manager.WaitForModeTransition(
          peloton::CHECKPOINT_STATUS_DONE_RECOVERY, true);
    }

    // start checkpointing mode after recovery
    if (peloton_checkpoint_mode != CHECKPOINT_TYPE_INVALID) {
      if (!checkpoint_manager.IsInCheckpointingMode()) {
        // Now, enter CHECKPOINTING mode
        checkpoint_manager.SetCheckpointStatus(
            peloton::CHECKPOINT_STATUS_CHECKPOINTING);
      }
    }
  }

  peloton_wait_timeout = state.wait_timeout;
  if (peloton_logging_mode != LOGGING_TYPE_INVALID) {
    // Launching a thread for logging
    if (!log_manager.IsInLoggingMode()) {
      // Set sync commit mode
      if (state.sync_commit == 1) {
        log_manager.SetSyncCommit(true);
      }
      log_manager.SetLogFileSizeLimit((unsigned int)state.file_size);
      log_manager.SetLogBufferCapacity((unsigned int)state.log_buffer_size);

      // Wait for standby mode
      std::thread(&peloton::logging::LogManager::StartStandbyMode, &log_manager)
          .detach();
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

// Main Entry Point
void RunBenchmark() {

  YCSBBootstrapLogger();

  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Run the workload
  auto stat = RunWorkload();

  WriteOutput(stat);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char** argv) {
  peloton::benchmark::ycsb::ParseArguments(argc, argv,
                                           peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
