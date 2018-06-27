//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// recycle_stack.cpp
//
// Identification: src/gc/recycle_stack.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gc/recycle_stack.h"

#include "common/logger.h"

namespace peloton {

namespace gc {

RecycleStack::~RecycleStack() {
  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acquire))
    ;

  auto curr = head_.next;

  // iterate through entire stack, remove all nodes
  while (curr != nullptr) {
    // acquire lock on curr
    while (curr->lock.test_and_set(std::memory_order_acquire))
      ;

    head_.next = curr->next;  // unlink curr
    // no need to release lock on curr because no one can be waiting on it
    // bceause we have lock on head_
    delete curr;

    curr = head_.next;
  }

  head_.lock.clear(std::memory_order_release);
}

void RecycleStack::Push(const ItemPointer &location) {
  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acquire))
    ;

  auto node = new Node{location, head_.next, ATOMIC_FLAG_INIT};
  head_.next = node;

  head_.lock.clear(std::memory_order_release);
}

ItemPointer RecycleStack::TryPop() {
  ItemPointer location = INVALID_ITEMPOINTER;

  LOG_TRACE("Trying to pop a recycled slot");

  // try to acquire head lock
  if (!head_.lock.test_and_set(std::memory_order_acquire)) {
    LOG_TRACE("Acquired head lock");
    auto node = head_.next;
    if (node != nullptr) {
      // try to acquire first node in list
      if (!node->lock.test_and_set(std::memory_order_acquire)) {
        LOG_TRACE("Acquired first node lock");
        head_.next = node->next;
        location = node->location;
        // no need to release lock on node because no one can be waiting on it
        // because we have lock on head_
        delete node;
      }
    }
    // release lock
    head_.lock.clear(std::memory_order_release);
  }

  return location;
}

uint32_t RecycleStack::RemoveAllWithTileGroup(const oid_t &tile_group_id) {
  uint32_t remove_count = 0;

  LOG_TRACE("Removing all recycled slots for TileGroup %u", tile_group_id);

  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acquire))
    ;

  auto prev = &head_;
  auto curr = prev->next;

  // iterate through entire stack, remove any nodes with matching tile_group_id
  while (curr != nullptr) {
    // acquire lock on curr
    while (curr->lock.test_and_set(std::memory_order_acquire))
      ;

    // check if we want to remove this node
    if (curr->location.block == tile_group_id) {
      prev->next = curr->next;  // unlink curr
      // no need to release lock on curr because no one can be waiting on it
      // bceause we have lock on prev
      delete curr;
      remove_count++;

      curr = prev->next;
      continue;  // need to check if null and acquire lock on new curr
    }

    // iterate
    prev->lock.clear(std::memory_order_release);
    prev = curr;
    curr = prev->next;
  }

  // prev was set to curr, which needs to be freed
  prev->lock.clear(std::memory_order_release);

  LOG_TRACE("Removed %u recycled slots for TileGroup %u", remove_count,
            tile_group_id);

  return remove_count;
}

}  // namespace gc
}  // namespace peloton
