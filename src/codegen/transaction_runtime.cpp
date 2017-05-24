//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.cpp
//
// Identification: src/codegen/transaction_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_runtime.h"

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace codegen {

// Perform a read operation for all tuples in the tile group in the given range
// TODO: Right now, we split this check into two loops: a visibility check and
//       the actual reading. Can this be merged?
uint32_t TransactionRuntime::PerformVectorizedRead(
    concurrency::Transaction &txn, storage::TileGroup &tile_group,
    uint32_t tid_start, uint32_t tid_end, uint32_t *selection_vector) {
  // Get the transaction manager
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Get the tile group header
  auto tile_group_header = tile_group.GetHeader();

  // Check visibility of tuples in the range [tid_start, tid_end), storing all
  // visible tuple IDs in the provided selection vector
  uint32_t out_idx = 0;
  for (uint32_t i = tid_start; i < tid_end; i++) {
    // Perform the visibility check
    auto visibility = txn_manager.IsVisible(&txn, tile_group_header, i);

    // Update the output position
    selection_vector[out_idx] = i;
    out_idx += (visibility == VisibilityType::OK);
  }

  //  uint32_t tile_group_idx = tile_group.GetTileGroupId();

  // Perform a read operation for every visible tuple we found
  //  uint32_t end_idx = out_idx;
  //  out_idx = 0;
  //  for (uint32_t idx = 0; idx < end_idx; idx++) {
  //    // Construct the item location
  //    ItemPointer location{tile_group_idx, selection_vector[idx]};
  //
  //    // Perform the read
  //    bool can_read = txn_manager.PerformRead(&txn, location);
  //
  //    // Update the selection vector and output position
  //    selection_vector[out_idx] = selection_vector[idx];
  //    out_idx += static_cast<uint32_t>(can_read);
  //  }

  return out_idx;
}

}  // namespace codegen
}  // namespace peloton
