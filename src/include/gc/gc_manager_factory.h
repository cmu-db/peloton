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
#include "common/types.h"

namespace peloton {
namespace gc {

class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    static GCManager gc_manager(GARBAGE_COLLECTION_TYPE_OFF);
    return gc_manager;
  }

  static void Configure(GarbageCollectionType gc_type) { gc_type_ = gc_type; }

  static GarbageCollectionType GetGCType() { return gc_type_; }

 private:
  // GC type
  static GarbageCollectionType gc_type_;
};

}  // namespace gc
}  // namespace peloton
