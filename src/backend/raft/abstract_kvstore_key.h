//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// kvstore_key.h
//
// Identification: src/backend/kvstore/kvstore_key.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <string>

namespace peloton {
namespace kvstore {

class KVStoreKey {
 public:
  /** @brief putEntry function to be overridden by derived class. */
  std::string kvstore_key;
};

}  // namespace kvstore
}  // namespace peloton
