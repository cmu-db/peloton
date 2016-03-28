//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// allocator.h
//
// Identification: src/backend/common/allocator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdlib.h>
#include <new>
#include <limits>
#include <cstddef>

namespace peloton {

// Custom allocator for tracking memory usage
template <class T>
class PelotonAllocator {
 public:
  typedef T value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;

  typedef T* pointer;
  typedef const T* const_pointer;

  typedef T& reference;
  typedef const T& const_reference;

  PelotonAllocator() throw() {}
  PelotonAllocator(const PelotonAllocator&) throw() {}
  template <class U>
  PelotonAllocator(const PelotonAllocator<U>&) throw() {}

  ~PelotonAllocator() throw() {}

  template <class U>
  struct rebind {
    typedef PelotonAllocator<U> other;
  };

  pointer address(reference r) const { return &r; }

  const_pointer address(const_reference r) const { return &r; }

  pointer allocate(size_type n, const void* = 0) {
    if (0 == n) return nullptr;

    // try to allocate
    pointer p = (pointer)malloc(n * sizeof(T));

    if (p == nullptr) throw std::bad_alloc();

    // update memory footprint
    memory_footprint += n * sizeof(T);

    return p;
  }

  void deallocate(pointer p, size_type n) {
    free(p);

    // update memory footprint
    memory_footprint -= n * sizeof(T);
  }

  void construct(pointer p, const T& val) { new (p) T(val); }

  void destroy(pointer p) { p->~T(); }

  size_type max_size() const {
    return std::numeric_limits<std::size_t>::max() / sizeof(T);
  }

  // Get the memory footprint
  size_t GetMemoryFootprint() const { return memory_footprint; }

 private:
  void operator=(const PelotonAllocator&);

  // Memory footprint
  size_t memory_footprint = 0;
};

template <class T>
bool operator==(const PelotonAllocator<T>& left,
                const PelotonAllocator<T>& right) {
  if (left.m_manager == right.m_manager) return true;
  return false;
}

template <class T>
bool operator!=(const PelotonAllocator<T>& left,
                const PelotonAllocator<T>& right) {
  if (left.m_manager != right.m_manager) return true;
  return false;
}

}  // namespace peloton
