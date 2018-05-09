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

//static constexpr size_t MAX_POP_ATTEMPTS = 5;

class RecycleStack {
 public:

  RecycleStack();

  ~RecycleStack();

  void Push(const ItemPointer &location);

  ItemPointer TryPop();

  size_t RemoveAllWithTileGroup(const oid_t &tile_group_id);

 private:

  struct Node {
    ItemPointer location;
    Node *next;
    std::atomic_flag lock;
  };

  Node head_;
}; // class RecycleStack
} // namespace gc
} // namespace peloton
