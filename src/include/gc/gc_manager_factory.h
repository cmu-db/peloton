//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.h
//
// Identification: src/include/gc/gc_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "gc/gc_manager.h"


namespace peloton {
namespace gc {

class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    switch (gc_type_) {

      case CONCURRENCY_TYPE_TIMESTAMP_ORDERING:
        // return TransactionLevelGCManager::GetInstance();

      default:
        return GCManager::GetInstance();
    }
  }

  static void Configure(GarbageCollectionType gc_type) { gc_type_ = gc_type; }

  static GarbageCollectionType GetGCType() { return gc_type_; }

 private:
  // GC type
  static GarbageCollectionType gc_type_;
};

}  // namespace gc
}  // namespace peloton
