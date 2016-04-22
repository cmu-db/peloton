//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.cpp
//
// Identification: src/backend/concurrency/gc_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gc_manager_factory.h"

namespace peloton {
namespace gc { GCType GCManagerFactory::gc_type_ = GC_TYPE_VACUUM; }
}
