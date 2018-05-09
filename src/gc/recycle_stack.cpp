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

#include "include/gc/recycle_stack.h"

namespace peloton {

namespace gc {

RecycleStack::RecycleStack() {
  head_.next = nullptr;
  head_.lock = ATOMIC_FLAG_INIT;
  head_.location = INVALID_ITEMPOINTER;
}

// unlinks and deletes all nodes in the stack
RecycleStack::~RecycleStack() {
  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acq_rel));

  Node *curr = head_.next;

  // iterate through entire stack, remove all nodes
  while (curr != nullptr) {

    // acquire lock on curr
    while(curr->lock.test_and_set(std::memory_order_acq_rel));

    head_.next = curr->next; // unlink curr
    // no need to release lock on curr because no one can be waiting on it
    // bceause we have lock on head_
    delete curr;

    curr = head_.next;
  }

  head_->lock.clear(std::memory_order_acq_rel);
}

// Used by GC Manager to add to recycle stack (can be slower)
void RecycleStack::Push(const ItemPointer &location) {

  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acq_rel));

  Node* node =  new Node{location, head_.next, ATOMIC_FLAG_INIT};
  head_.next = node;

  head_.lock.clear(std::memory_order_acq_rel);
}

// Used by GetRecycledTupleSlot to get an empty slot (must be fast)
// try to acquire the lock and pop an itempointer off the stack
// TODO: Consider trying MAX_POP_ATTEMPTS
ItemPointer RecycleStack::TryPop() {
  ItemPointer location = INVALID_ITEMPOINTER;

  // try to acquire head lock
  if (!head_.lock.test_and_set(std::memory_order_acq_rel)) {
    Node* node = head_.next;
    if (node != nullptr) {
      // try to acquire first node in list
      if (!node->lock.test_and_set(std::memory_order_acq_rel)) {
        head_.next = node->next;
        location = node->location;
        // no need to release lock on node because no one can be waiting on it
        // because we have lock on head_
        delete node;
      }
    }
    // release lock
    head_.lock.clear(std::memory_order_acq_rel);
  }

  return location;
}

size_t RecycleStack::RemoveAllWithTileGroup(const oid_t &tile_group_id) {
  size_t remove_count = 0;

  // acquire head lock
  while (head_.lock.test_and_set(std::memory_order_acq_rel));

  Node *prev = &head_;
  Node *curr = prev->next;

  // iterate through entire stack, remove any nodes with matching tile_group_id
  while (curr != nullptr) {

    // acquire lock on curr
    while(curr->lock.test_and_set(std::memory_order_acq_rel));

    // check if we want to remove this node
    if (curr->location.block == tile_group_id) {
      prev->next = curr->next; // unlink curr
      // no need to release lock on curr because no one can be waiting on it
      // bceause we have lock on prev
      delete curr;
      remove_count++;

      curr = prev->next;
      continue; // need to check if null and acquire lock on new curr
    }

    // iterate
    prev->lock.clear(std::memory_order_acq_rel);
    prev = curr;
    curr = prev->next;
  }

  // prev was set to curr, which needs to be freed
  prev->lock.clear(std::memory_order_acq_rel);

  return remove_count;
}

} // namespace gc
} // namespace peloton
