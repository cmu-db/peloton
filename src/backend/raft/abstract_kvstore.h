//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_kvstore.h
//
// Identification: src/backend/raft/abstract_kvstore.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_kvstore_entry.h"

namespace peloton {
namespace kvstore {

class AbstractKVStore {
 public:
  /** @brief putEntry function to be overridden by derived class. */
  virtual bool PutEntry();

  /** @brief getEntry function to be overridden by derived class. */
  virtual KVStoreEntry GetEntry();

  /** @brief removeEntry function to be overridden by derived class. */
  virtual bool RemoveEntry();

  /** @brief containsEntry function to be overridden by derived class. */
  virtual bool ContainsEntry();
};

}  // namespace kvstore
}  // namespace peloton
