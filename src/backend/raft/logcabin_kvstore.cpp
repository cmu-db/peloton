//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logcabin_kvstore.cpp
//
// Identification: src/backend/raft/logcabin_kvstore.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/raft/logcabin_kvstore.h"

namespace peloton {
namespace kvstore {

/**
 * @brief Enters an object of type 'KVSEntry' in the Key Value Store
 *
 * @return True or false based on success of entry input.
 */
bool LogcabinKVStore::PutEntry(KVStoreKey *key, KVStoreEntry *entry) {
  return false;
}

/**
 * @brief Fetches an entry from the Key Value Store
 *
 * @return Object of type Entry.
 */
KVStoreEntry *LogcabinKVStore::GetEntry(KVStoreKey *key) {
  KVStoreEntry *entry = new KVStoreEntry();
  return entry;
}

/**
 * @brief Removes an entry from the Key Value Store
 *
 * @return True or false based on binding.
 */
bool LogcabinKVStore::RemoveEntry(KVStoreKey *key) {
  return false;
}

/**
 * @brief Checks if an entry exists in Key Value Store
 *
 * @return True or false based on existence of entry
 */
bool LogcabinKVStore::ContainsEntry(KVStoreKey *key) {
  return false;
}

}  // End kvstore namespace
}  // End peloton namespace
