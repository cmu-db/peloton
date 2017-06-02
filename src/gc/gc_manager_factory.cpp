//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.cpp
//
// Identification: src/gc/gc_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gc/gc_manager_factory.h"

namespace peloton {
namespace gc {

GarbageCollectionType GCManagerFactory::gc_type_ = GarbageCollectionType::ON;

int GCManagerFactory::gc_thread_count_ = 1;

}  // namespace gc
}  // namespace peloton
