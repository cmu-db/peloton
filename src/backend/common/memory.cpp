//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memory.cpp
//
// Identification: src/backend/common/memory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <new>
#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include <execinfo.h>
#include <unistd.h>

#include "backend/common/macros.h"
#include "backend/common/logger.h"

#define JEMALLOC_NO_DEMANGLE
#include <jemalloc/jemalloc.h>

namespace peloton {

void *do_allocation(size_t size, bool do_throw){

  // allocations more than 32MB are suspect
  if (unlikely_branch(size > (1 << 25))) {
    LOG_WARN("Warning: Large memory allocation (%lu bytes)", size);
    void *addrs[128];
    const size_t naddrs = backtrace(addrs, ARRAY_NELEMS(addrs));
    backtrace_symbols_fd(addrs, naddrs, STDERR_FILENO);
  }

  void *location = je_malloc(size);
  if (unlikely_branch(!location && do_throw)) {
    throw std::bad_alloc();
  }

  return location;
}

void do_deletion(void *location) {
  je_free(location);
}

}  // End peloton namespace

void* operator new(size_t size) throw (std::bad_alloc){
  return peloton::do_allocation(size, true);
}

void* operator new(size_t size, const std::nothrow_t&) throw () {
  return peloton::do_allocation(size, false);
}

void* operator new[](size_t size) throw (std::bad_alloc) {
  return peloton::do_allocation(size, true);
}

void* operator new[](size_t size, std::nothrow_t &) throw () {
  return peloton::do_allocation(size, false);
}

void operator delete(void *location) throw () {
  return peloton::do_deletion(location);
}

void operator delete(void *location, const std::nothrow_t &) throw () {
  return peloton::do_deletion(location);
}

void operator delete[](void *location) throw () {
  return peloton::do_deletion(location);
}

void operator delete[](void *location, const std::nothrow_t &) throw () {
  return peloton::do_deletion(location);
}
