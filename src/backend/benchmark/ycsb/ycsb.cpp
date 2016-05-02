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
#include <iomanip>

#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/common/assert.h"
#include "backend/gc/gc_manager_factory.h"
#include "backend/common/types.h"

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

#include "backend/logging/log_manager.h"

extern LoggingType peloton_logging_mode;

extern int64_t peloton_wait_timeout;

extern int peloton_flush_frequency_micros;

namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;
extern storage::DataTable *user_table;

std::ofstream out("outputfile.summary", std::ofstream::out);

static void WriteOutput() {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d %d %d %d %d :: %lf tps, %lf", state.update_ratio,
           state.scale_factor, state.column_count, state.logging_enabled,
           state.sync_commit, state.file_size, state.checkpointer,
           state.throughput, state.abort_rate);

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << " ";
  out << state.backend_count << " ";
  out << state.logging_enabled << " ";
  out << state.sync_commit << " ";
  out << state.wait_timeout << " ";
  out << state.file_size << " ";
  out << state.checkpointer << "\n";

  for (size_t round_id = 0; round_id < state.snapshot_throughput.size();
       ++round_id) {
    out << "[" << std::setw(3) << std::left
        << state.snapshot_duration * round_id << " - " << std::setw(3)
        << std::left << state.snapshot_duration * (round_id + 1)
        << " s]: " << state.snapshot_throughput[round_id] << " "
        << state.snapshot_abort_rate[round_id] << "\n";
  }

  out << state.throughput << " ";
  out << state.abort_rate << "\n";
  out.flush();
  out.close();
}

// Validate that MVCC storage is correct, it assumes an old-to-new chain
// Invariants
// 1. Transaction id should either be INVALID_TXNID or INITIAL_TXNID
// 2. Begin commit id should <= end commit id
static void ValidateMVCC() {
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto &catalog_manager = catalog::Manager::GetInstance();
  gc_manager.StopGC();
  LOG_TRACE("Validating MVCC storage");
  int tile_group_count = user_table->GetTileGroupCount();
  LOG_TRACE("The table has %d tile groups in the table", tile_group_count);

  for (int tile_group_offset = 0; tile_group_offset < tile_group_count;
       tile_group_offset++) {
    LOG_TRACE("Validate tile group #%d", tile_group_offset);
    auto tile_group = user_table->GetTileGroup(tile_group_offset);
    auto tile_group_header = tile_group->GetHeader();
    size_t tuple_count = tile_group->GetAllocatedTupleCount();
    LOG_TRACE("Tile group #%d has allocated %lu tuples", tile_group_offset,
              tuple_count);

    // 1. Transaction id should either be INVALID_TXNID or INITIAL_TXNID
    for (oid_t tuple_slot = 0; tuple_slot < tuple_count; tuple_slot++) {
      txn_id_t txn_id = tile_group_header->GetTransactionId(tuple_slot);
      CHECK_M(txn_id == INVALID_TXN_ID || txn_id == INITIAL_TXN_ID,
              "Transaction id is not INVALID_TXNID or INITIAL_TXNID");
    }

    LOG_TRACE("[OK] All tuples have valid txn id");

    // double avg_version_chain_length = 0.0;
    for (oid_t tuple_slot = 0; tuple_slot < tuple_count; tuple_slot++) {
      txn_id_t txn_id = tile_group_header->GetTransactionId(tuple_slot);
      cid_t begin_cid = tile_group_header->GetBeginCommitId(tuple_slot);
      cid_t end_cid = tile_group_header->GetEndCommitId(tuple_slot);
      ItemPointer next_location =
          tile_group_header->GetNextItemPointer(tuple_slot);
      ItemPointer prev_location =
          tile_group_header->GetPrevItemPointer(tuple_slot);

      // 2. Begin commit id should <= end commit id
      CHECK_M(begin_cid <= end_cid,
              "Tuple begin commit id is less than or equal to end commit id");

      // This test assumes a oldest-to-newest version chain
      if (txn_id != INVALID_TXN_ID) {
        CHECK(begin_cid != MAX_CID);

        // The version is an oldest version
        if (prev_location.IsNull()) {
          if (next_location.IsNull()) {
            CHECK_M(end_cid == MAX_CID,
                    "Single version has a non MAX_CID end commit time");
          } else {
            cid_t prev_end_cid = end_cid;
            ItemPointer prev_location(tile_group->GetTileGroupId(), tuple_slot);
            while (!next_location.IsNull()) {
              auto next_tile_group =
                  catalog_manager.GetTileGroup(next_location.block);
              auto next_tile_group_header = next_tile_group->GetHeader();

              txn_id_t next_txn_id = next_tile_group_header->GetTransactionId(
                  next_location.offset);

              if (next_txn_id == INVALID_TXN_ID) {
                // If a version in the version chain has a INVALID_TXN_ID, it
                // must be at the tail
                // of the chain. It is either because we have deleted a tuple
                // (so append a invalid tuple),
                // or because this new version is aborted.
                CHECK_M(next_tile_group_header->GetNextItemPointer(
                                                  next_location.offset)
                            .IsNull(),
                        "Invalid version in a version chain and is not delete");
              }

              cid_t next_begin_cid = next_tile_group_header->GetBeginCommitId(
                  next_location.offset);
              cid_t next_end_cid =
                  next_tile_group_header->GetEndCommitId(next_location.offset);

              // 3. Timestamp consistence
              if (next_begin_cid == MAX_CID) {
                // It must be an aborted version, it should be at the end of the
                // chain
                CHECK_M(next_tile_group_header->GetNextItemPointer(
                                                  next_location.offset)
                            .IsNull(),
                        "Version with MAX_CID begin cid is not version tail");
              } else {
                CHECK_M(prev_end_cid == next_begin_cid,
                        "Prev end commit id should equal net begin commit id");
                ItemPointer next_prev_location =
                    next_tile_group_header->GetPrevItemPointer(
                        next_location.offset);

                // 4. Version doubly linked list consistency
                CHECK_M(next_prev_location.offset == prev_location.offset &&
                            next_prev_location.block == prev_location.block,
                        "Next version's prev version does not match");
              }

              prev_location = next_location;
              prev_end_cid = next_end_cid;
              next_location = next_tile_group_header->GetNextItemPointer(
                  next_location.offset);
            }

            // Now prev_location is at the tail of the version chain
            ItemPointer last_location = prev_location;
            auto last_tile_group =
                catalog_manager.GetTileGroup(last_location.block);
            auto last_tile_group_header = last_tile_group->GetHeader();
            // txn_id_t last_txn_id =
            // last_tile_group_header->GetTransactionId(last_location.offset);
            cid_t last_end_cid =
                last_tile_group_header->GetEndCommitId(last_location.offset);
            CHECK_M(
                last_tile_group_header->GetNextItemPointer(last_location.offset)
                    .IsNull(),
                "Last version has a next pointer");

            CHECK_M(last_end_cid == MAX_CID,
                    "Last version doesn't end with MAX_CID");
          }
        }
      } else {
        CHECK_M(tile_group_header->GetNextItemPointer(tuple_slot).IsNull(),
                "Invalid tuple must not have next item pointer");
      }
    }
  }
  LOG_INFO("[OK] oldest-to-newest version chain validated");

  gc_manager.StartGC();
}

inline void YCSBBootstrapLogger() {
  peloton_logging_mode = LOGGING_TYPE_NVM_WAL;
  peloton_wait_timeout = state.wait_timeout;
  peloton_flush_frequency_micros = state.flush_freq;

  auto& log_manager = peloton::logging::LogManager::GetInstance();

  if (state.checkpointer != 0) {
    peloton_checkpoint_mode = CHECKPOINT_TYPE_NORMAL;
    auto& checkpoint_manager =
        peloton::logging::CheckpointManager::GetInstance();

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

  if (state.logging_enabled <= 0) return;

  // Set sync commit mode
  if (state.sync_commit == 1) {
    log_manager.SetSyncCommit(true);
  }
  log_manager.SetLogFileSizeLimit((unsigned int)state.file_size);
  log_manager.SetLogBufferCapacity((unsigned int)state.log_buffer_size);

  // Wait for standby mode
  std::thread(&peloton::logging::LogManager::StartStandbyMode, &log_manager)
      .detach();
  log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_STANDBY, true);

  // Clean up database tile state before recovery from checkpoint
  log_manager.PrepareRecovery();

  // Do any recovery
  log_manager.StartRecoveryMode();

  // Wait for logging mode
  log_manager.WaitForModeTransition(peloton::LOGGING_STATUS_TYPE_LOGGING, true);

  // Done recovery
  log_manager.DoneRecovery();
}

// Main Entry Point
void RunBenchmark() {

  YCSBBootstrapLogger();

  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Validate MVCC storage
  ValidateMVCC();

  // Run the workload
  RunWorkload();

  // Validate MVCC storage
  ValidateMVCC();

  WriteOutput();

  auto& log_manager = peloton::logging::LogManager::GetInstance();
  if (log_manager.IsInLoggingMode()) {
    log_manager.TerminateLoggingMode();
  }
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
