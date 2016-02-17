//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// kvstore_entry.h
//
// Identification: src/backend/kvstore/kvstore_entry.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

namespace peloton {
namespace kvstore {

class KVStoreEntry {
 public:
  std::string kvstore_entry;
};

}  // namespace kvstore
}  // namespace peloton
