//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions.cpp
//
// Identification: src/codegen/runtime_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/runtime_functions.h"

#include <nmmintrin.h>

#include "murmur3/MurmurHash3.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/synchronization/count_down_latch.h"
#include "expression/abstract_expression.h"
#include "storage/data_table.h"
#include "storage/layout.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {
namespace codegen {

uint64_t RuntimeFunctions::HashMurmur3(const char *buf, uint64_t length,
                                       uint64_t seed) {
  return MurmurHash3_x86_32(buf, static_cast<uint32_t>(length),
                            static_cast<uint32_t>(seed));
}

#define CRC32(op, crc, type, buf, len)                   \
  do {                                                   \
    for (; (len) >= sizeof(type);                        \
         (len) -= sizeof(type), (buf) += sizeof(type)) { \
      (crc) = op((crc), *(type *)buf);                   \
    }                                                    \
  } while (0)

//===----------------------------------------------------------------------===//
// Calculate the CRC64 checksum over the given buffer of the provided length
// using the provided CRC as the initial/running CRC value
//===----------------------------------------------------------------------===//
uint64_t RuntimeFunctions::HashCrc64(const char *buf, uint64_t length,
                                     uint64_t crc) {
  // If the string is empty, return the CRC calculated so far
  if (length == 0) {
    return crc;
  }

#if defined(__x86_64__) || defined(_M_X64)
  // Try to each up as many 8-byte values as possible
  CRC32(_mm_crc32_u64, crc, uint64_t, buf, length);
#endif
  // Now we perform CRC in 4-byte, then 2-byte chunks.  Finally, we process
  // what remains in byte chunks.
  CRC32(_mm_crc32_u32, crc, uint32_t, buf, length);
  CRC32(_mm_crc32_u16, crc, uint16_t, buf, length);
  CRC32(_mm_crc32_u8, crc, uint8_t, buf, length);
  // Done
  return crc;
}

//===----------------------------------------------------------------------===//
// Get the tile group with the given index from the table.
//
// TODO: DataTable::GetTileGroup() returns a std::shared_ptr<> that we strip
//       off. This means we could be touching free'd data. This must be fixed.
//===----------------------------------------------------------------------===//
storage::TileGroup *RuntimeFunctions::GetTileGroup(storage::DataTable *table,
                                                   uint64_t tile_group_index) {
  auto tile_group = table->GetTileGroup(tile_group_index);
  return tile_group.get();
}

//===----------------------------------------------------------------------===//
// Fills in the Predicate Array for the Zone Map to compare against.
// Predicates are converted into an array of struct.
// Each struct contains the column id, operator id and predicate value.
//===----------------------------------------------------------------------===//
void RuntimeFunctions::FillPredicateArray(
    const expression::AbstractExpression *expr,
    storage::PredicateInfo *predicate_array) {
  const std::vector<storage::PredicateInfo> *parsed_predicates;
  parsed_predicates = expr->GetParsedPredicates();
  size_t num_preds = parsed_predicates->size();
  size_t i;
  for (i = 0; i < num_preds; i++) {
    predicate_array[i].col_id = (*parsed_predicates)[i].col_id;
    predicate_array[i].comparison_operator =
        (*parsed_predicates)[i].comparison_operator;
    predicate_array[i].predicate_value =
        (*parsed_predicates)[i].predicate_value;
  }
  auto temp_expr = (expression::AbstractExpression *)expr;
  temp_expr->ClearParsedPredicates();
}

//===----------------------------------------------------------------------===//
// For every column in the tile group, fill out the layout information for the
// column in the provided 'infos' array.  Specifically, we need a pointer to
// where the first value of the column can be found, and the amount of bytes
// to skip over to find successive values of the column.
//===----------------------------------------------------------------------===//
void RuntimeFunctions::GetTileGroupLayout(const storage::TileGroup *tile_group,
                                          ColumnLayoutInfo *infos,
                                          UNUSED_ATTRIBUTE uint32_t num_cols) {
  const auto &layout = tile_group->GetLayout();
  UNUSED_ATTRIBUTE oid_t last_col_idx = INVALID_OID;

  auto tile_map = layout.GetTileMap();
  // Find the mapping for each tile in the layout.
  for (auto tile_entry : tile_map) {
    // Get the tile schema.
    auto tile_idx = tile_entry.first;
    auto *tile = tile_group->GetTile(tile_idx);
    auto tile_schema = tile->GetSchema();
    // Map the current column to a tile and a column offset in the tile.
    for (auto column_entry : tile_entry.second) {
      // Now grab the column information
      oid_t col_idx = column_entry.first;
      oid_t tile_col_offset = column_entry.second;
      // Ensure that the col_idx is within the num_cols range
      PELOTON_ASSERT(col_idx < num_cols);
      infos[col_idx].column =
          tile->GetTupleLocation(0) + tile_schema->GetOffset(tile_col_offset);
      infos[col_idx].stride = tile_schema->GetLength();
      infos[col_idx].is_columnar = tile_schema->GetColumnCount() == 1;
      last_col_idx = col_idx;
      LOG_TRACE("Col [%u] start: %p, stride: %u, columnar: %s", col_idx,
                infos[col_idx].column, infos[col_idx].stride,
                infos[col_idx].is_columnar ? "true" : "false");
    }
  }
  // Ensure that ColumnLayoutInfo for each column has been populated.
  PELOTON_ASSERT((last_col_idx != INVALID_OID) &&
                 (last_col_idx == (num_cols - 1)));
}

void RuntimeFunctions::ExecuteTableScan(
    void *query_state, executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t db_oid, uint32_t table_oid, void *f) {
  //    void (*scanner)(void *, void *, uint64_t, uint64_t)) {
  using ScanFunc = void (*)(void *, void *, uint64_t, uint64_t);
  auto *scanner = reinterpret_cast<ScanFunc>(f);

  // The worker pool
  auto &worker_pool = threadpool::MonoQueuePool::GetExecutionInstance();

  // Pull out the data table
  auto *sm = storage::StorageManager::GetInstance();
  storage::DataTable *table = sm->GetTableWithOid(db_oid, table_oid);
  uint32_t num_tilegroups = static_cast<uint32_t>(table->GetTileGroupCount());

  // Determine the number of tasks to generate. In this case, we use:
  // num_tasks := num_tile_groups / num_workers
  uint32_t num_tasks = std::min(worker_pool.NumWorkers(), num_tilegroups);
  uint32_t num_tilegroups_per_task = num_tilegroups / num_tasks;

  // Allocate states for each task
  thread_states.Allocate(num_tasks);

  // Create count down latch
  common::synchronization::CountDownLatch latch(num_tasks);

  // Now, submit the tasks
  for (uint32_t task_id = 0; task_id < num_tasks; task_id++) {
    bool last_task = task_id == num_tasks - 1;
    auto tilegroup_start = task_id * num_tilegroups_per_task;
    auto tilegroup_stop =
        last_task ? num_tilegroups : tilegroup_start + num_tilegroups_per_task;
    auto work = [&query_state, &thread_states, &scanner, &latch, task_id,
                 tilegroup_start, tilegroup_stop]() {
      LOG_INFO("Task-%u scanning tile groups [%u-%u)", task_id, tilegroup_start,
               tilegroup_stop);

      // Time this
      Timer<std::ratio<1, 1000> > timer;
      timer.Start();

      // Pull out this task's thread state
      auto thread_state = thread_states.AccessThreadState(task_id);

      // Invoke scan function
      scanner(query_state, thread_state, tilegroup_start, tilegroup_stop);

      // Count down latch
      latch.CountDown();

      // Log stuff
      timer.Stop();
      LOG_INFO("Task-%u done scanning (%.2lf ms) ...", task_id,
               timer.GetDuration());
    };
    worker_pool.SubmitTask(work);
  }

  // Wait for everything to finish
  // TODO(pmenon): Loop await, checking for query error or cancellation
  latch.Await(0);
}

void RuntimeFunctions::ThrowDivideByZeroException() {
  throw DivideByZeroException("ERROR: division by zero");
}

void RuntimeFunctions::ThrowOverflowException() {
  throw std::overflow_error("ERROR: overflow");
}

}  // namespace codegen
}  // namespace peloton