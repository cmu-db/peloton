//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// recycle_stack.h
//
// Identification: src/include/gc/recycle_stack.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/item_pointer.h"

namespace peloton {

namespace gc {

/**
 * @brief Concurrent stack for the Garbage Collector to store recycled tuples
 *
 * The goals of this structure are:
 *  -# Provide fast, best-effort removal of recycled ItemPointers on the
 *      critical path for worker threads
 *  -# Provide possibly-slow, guaranteed insertion of recycled ItemPointers
 *      for background GC threads
 *  -# Provide possibly-slow, guaranteed correct removal of recycled
 *      ItemPointers for background GC threads
 *  -# Thread-safe for multiple GC threads and multiple worker threads
 */
class RecycleStack {
 public:

  /**
   * @return Empty RecycleStack
   */
  RecycleStack() {};

  /**
   * @brief Removes all elements from the stack
   */
  ~RecycleStack();

  /**
   * @brief Adds the provided ItemPointer to the top of the stack
   *
   * Intended for the Garbage Collector to use in a background thread so
   * performance is not critical. Will spin on the head lock until acquired
   *
   * @param location[in] ItemPointer to be added to the top of the stack
   */
  void Push(const ItemPointer &location);

  /**
 * @brief Attempts to remove an ItemPointer from the top of the stack
 *
 * Intended for the critical path during Insert by a worker thread so
 * performance is critical. Will not spin on the head lock until acquired
 *
 * @return ItemPointer from the top of the stack. Can be an
 * INVALID_ITEMPOINTER if stack is empty or failed to acquire head lock
 */
  ItemPointer TryPop();

  /**
   * @brief Removes all ItemPointers from the RecycleStack belonging to
   * the provded TileGroup oid.
   *
   * Intended for the Garbage Collector to use in a background thread
   * so performance is not critical. Will spin on the head lock until
   * acquired, and then iterate through the Recycle Stack using hand
   * over hand locking
   *
   * @param[in] tile_group_id The global oid of the TileGroup that
   * should have all of its ItemPointers removed from the RecycleStack
   *
   * @return Number of elements removed from the stack. Useful for
   * debugging
   */
  size_t RemoveAllWithTileGroup(const oid_t &tile_group_id);

 private:

  struct Node {
    ItemPointer location;
    Node *next;
    std::atomic_flag lock;
  };

  Node head_{INVALID_ITEMPOINTER, nullptr, ATOMIC_FLAG_INIT};
}; // class RecycleStack
} // namespace gc
} // namespace peloton
