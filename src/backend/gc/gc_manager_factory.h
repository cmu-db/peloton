//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.h
//
// Identification: src/backend/concurrency/gc_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/gc/cooperative_gc.h"
#include "backend/gc/vacuum_gc.h"
#include "backend/gc/off_gc.h"

namespace peloton {
namespace gc {

class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    switch (gc_type_) {
      case GC_TYPE_CO:
        return Cooperative_GCManager::GetInstance();
      case GC_TYPE_VACUUM:
        return Vacuum_GCManager::GetInstance();
      case GC_TYPE_OFF:
        return Off_GCManager::GetInstance();
      default:
        return Cooperative_GCManager::GetInstance();
    }
  }

  static void Configure(GCType gc_type) { gc_type_ = gc_type; }

  static GCType GetGCType() { return gc_type_; }

 private:

  // GC type
  static GCType gc_type_;
};

} // namespace gc
} // namespace peloton
