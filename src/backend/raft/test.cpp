//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// test.cpp
//
// Identification: src/backend/raft/test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logcabin_kvstore.h"
using namespace peloton;
using namespace kvstore;

int main(void) {
  KVStoreKey *key = new KVStoreKey();
  KVStoreEntry *entry = new KVStoreEntry();
  LogcabinKVStore *l = new LogcabinKVStore();
  l->PutEntry(key, entry);
  l->GetEntry(key);
  l->ContainsEntry(key);
  l->RemoveEntry(key);
}
