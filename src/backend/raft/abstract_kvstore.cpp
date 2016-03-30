//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_kvstore.cpp
//
// Identification: src/backend/raft/abstract_kvstore.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_kvstore.h"

namespace peloton {
namespace kvstore {

/**
 * @brief Enters an object of type 'KVSEntry' in the Key Value Store
 *
 * @return True or false based on success of entry input.
 */
bool AbstractKVStore::PutEntry() {}

/**
 * @brief Fetches an entry from the Key Value Store
 *
 * @return Object of type Entry.
 */
KVStoreEntry AbstractKVStore::GetEntry() {}

/**
 * @brief Removes an entry from the Key Value Store
 *
 * @return True or false based on binding.
 */
bool AbstractKVStore::RemoveEntry() {}

/**
 * @brief Checks if an entry exists in Key Value Store
 *
 * @return True or false based on existence of entry
 */
bool AbstractKVStore::ContainsEntry() {}

}  // namespace kvstore
}  // namespace peloton
