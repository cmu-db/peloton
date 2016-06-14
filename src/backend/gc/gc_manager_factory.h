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
#include "backend/gc/n2o_gc.h"

namespace peloton {
namespace gc {

class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    switch (gc_type_) {
      case GC_TYPE_CO:
        return Cooperative_GCManager::GetInstance();
      case GC_TYPE_VACUUM:
        return Vacuum_GCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_N2O:
        return N2O_GCManager::GetInstance(gc_thread_count_);
      case GC_TYPE_OFF:
        return Off_GCManager::GetInstance();
      default:
        return Cooperative_GCManager::GetInstance();
    }
  }

  static void Configure(GCType gc_type, int thread_count = default_gc_thread_count_) {
    gc_type_ = gc_type;
    gc_thread_count_ = thread_count;
  }

  static GCType GetGCType() { return gc_type_; }

 private:

  // GC type
  static GCType gc_type_;

  // GC thread count
  static int gc_thread_count_;
  const static int default_gc_thread_count_ = 1;
};
} // namespace gc
} // namespace peloton
