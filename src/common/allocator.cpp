//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// allocator.cpp
//
// Identification: src/common/allocator.cpp
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

#include "common/stack_trace.h"

// We will use jemalloc at link time. jemalloc library has already mangled the symbols
// to be malloc, calloc, etc.

namespace peloton {

void *do_allocation(size_t size, bool do_throw) {
  void *location = malloc(size);
  if (!location && do_throw) {
    throw std::bad_alloc();
  }

  return location;
}

void do_deletion(void *location) { free(location); }

}  // namespace peloton

void *operator new(size_t size) {
  return peloton::do_allocation(size, true);
}

void *operator new(size_t size, const std::nothrow_t &) noexcept {
  return peloton::do_allocation(size, false);
}

void *operator new[](size_t size) {
  return peloton::do_allocation(size, true);
}

void *operator new[](size_t size, std::nothrow_t &) noexcept {
  return peloton::do_allocation(size, false);
}

void operator delete(void *location) noexcept {
  return peloton::do_deletion(location);
}

void operator delete(void *location, __attribute__ ((unused)) size_t size) noexcept {
  return peloton::do_deletion(location);
}

void operator delete(void *location, const std::nothrow_t &) noexcept {
  return peloton::do_deletion(location);
}

void operator delete[](void *location) noexcept {
  return peloton::do_deletion(location);
}

void operator delete[](void *location, __attribute__ ((unused)) size_t size) noexcept {
  return peloton::do_deletion(location);
}

void operator delete[](void *location, const std::nothrow_t &) noexcept {
  return peloton::do_deletion(location);
}
