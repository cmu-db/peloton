//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logcabin_kvstore.h
//
// Identification: src/backend/raft/logcabin_kvstore.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_kvstore.h"
#include "abstract_kvstore_entry.h"
#include "abstract_kvstore_key.h"

namespace peloton {
namespace kvstore {

class LogcabinKVStore : public AbstractKVStore {
 public:
  /**
   * @brief Enters an object of type 'KVSEntry' in the Key Value Store
   *
   * @return True or false based on success of entry input.
   */
  bool PutEntry(KVStoreKey *key, KVStoreEntry *entry);

  /**
   * @brief Fetches an entry from the Key Value Store
   *
   * @return Object of type Entry.
   */
  KVStoreEntry *GetEntry(KVStoreKey *key);

  /**
   * @brief Removes an entry from the Key Value Store
   *
   * @return True or false based on binding.
   */
  bool RemoveEntry(KVStoreKey *key);

  /**
   * @brief Checks if an entry exists in Key Value Store
   *
   * @return True or false based on existence of entry
   */
  bool ContainsEntry(KVStoreKey *key);
};

}  // End kvstore namespace
}  // End peloton namespace
