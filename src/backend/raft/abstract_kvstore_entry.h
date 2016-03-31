//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_kvstore_entry.h
//
// Identification: src/backend/raft/abstract_kvstore_entry.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
