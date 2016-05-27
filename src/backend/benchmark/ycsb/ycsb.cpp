//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb.cpp
//
// Identification: benchmark/ycsb/ycsb.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#undef NDEBUG

#include <iostream>
#include <fstream>
#include <iomanip>

#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
//#include "backend/common/assert.h"
#include "backend/gc/gc_manager_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"
#include "backend/benchmark/ycsb/ycsb_workload.h"

#define CHECK(x)                                                          \
  do {                                                                    \
    if (!(x)) {                                                           \
      ::fprintf(stderr, "CHECK failed: %s at %s:%d in function %s\n", #x, \
                __FILE__, __LINE__, __PRETTY_FUNCTION__);                 \
      ::abort();                                                          \
    }                                                                     \
  } while (0)
  

#define CHECK_M(x, message, args...)                                           \
  do {                                                                         \
    if (!(x)) {                                                                \
      ::fprintf(stderr,                                                        \
                "CHECK failed: %s at %s:%d in function %s\n" message "\n", #x, \
                __FILE__, __LINE__, __PRETTY_FUNCTION__, ##args);              \
      ::abort();                                                               \
    }                                                                          \
  } while (0)


namespace peloton {
namespace benchmark {
namespace ycsb {

configuration state;
extern storage::DataTable *user_table;

std::ofstream out("outputfile.summary", std::ofstream::out);

static void WriteOutput() {
  LOG_INFO("----------------------------------------------------------");
  LOG_INFO("%lf %d %d :: %lf tps, %lf", state.update_ratio, state.scale_factor,
           state.column_count, state.throughput, state.abort_rate);

  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << state.column_count << "\n";

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
              "(%u,%u) Transaction id %lu(%lx) is not INVALID_TXNID or INITIAL_TXNID", tile_group->GetTileGroupId(), tuple_slot, txn_id, txn_id);
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
              // It must be an aborted version, it shouldn't exist in version chain
              CHECK_M(next_begin_cid != MAX_CID,
                      "Aborted version shouldn't be at version chain");

              // 4. Version doubly linked list consistency
              CHECK_M(prev_end_cid == next_begin_cid,
                      "Prev end commit id should equal net begin commit id");
              ItemPointer next_prev_location =
                  next_tile_group_header->GetPrevItemPointer(
                      next_location.offset);


              CHECK_M(next_prev_location.offset == prev_location.offset &&
                          next_prev_location.block == prev_location.block,
                      "Next version's prev version does not match");

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

// Main Entry Point
void RunBenchmark() {
  gc::GCManagerFactory::Configure(state.gc_protocol);
  concurrency::TransactionManagerFactory::Configure(state.protocol);
  // Create and load the user table
  CreateYCSBDatabase();

  LoadYCSBDatabase();

  // Validate MVCC storage
  if (state.protocol != CONCURRENCY_TYPE_OCC_N2O && state.protocol != CONCURRENCY_TYPE_OCC_RB) {
    ValidateMVCC();
  }

  // Run the workload
  RunWorkload();

  // Validate MVCC storage
  if (state.protocol != CONCURRENCY_TYPE_OCC_N2O && state.protocol != CONCURRENCY_TYPE_OCC_RB) {
    ValidateMVCC();
  }

  WriteOutput();
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::ycsb::ParseArguments(argc, argv,
                                           peloton::benchmark::ycsb::state);

  peloton::benchmark::ycsb::RunBenchmark();

  return 0;
}
