
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer.h
//
// Identification: src/include/common/item_pointer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "type/types.h"

namespace peloton {

// logical physical location
struct ItemPointer {
  // block
  oid_t block;

  // 0-based offset within block
  oid_t offset;

  ItemPointer() : block(INVALID_OID), offset(INVALID_OID) {}

  ItemPointer(oid_t block, oid_t offset) : block(block), offset(offset) {}

  bool IsNull() const {
    return (block == INVALID_OID && offset == INVALID_OID);
  }

  bool operator<(const ItemPointer &rhs) const {
    if (block != rhs.block) {
      return block < rhs.block;
    } else {
      return offset < rhs.offset;
    }
  }

} __attribute__((__aligned__(8))) __attribute__((__packed__));

extern ItemPointer INVALID_ITEMPOINTER;

struct ItemPointerHasher {
  size_t operator()(const ItemPointer &item) const {
    return std::hash<oid_t>()(item.block) ^ std::hash<oid_t>()(item.offset);
  }
};

bool AtomicUpdateItemPointer(ItemPointer *src_ptr, const ItemPointer &value);

}  // namespace peloton
