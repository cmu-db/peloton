/*-------------------------------------------------------------------------
 *
 * shm_allocator.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/common/shm_allocator.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace peloton {

//===--------------------------------------------------------------------===//
// SHM Allocator
//===--------------------------------------------------------------------===//

template <class T>
class SHMAllocator {

 public:
  typedef T          value_type;
  typedef size_t     size_type;
  typedef ptrdiff_t  difference_type;

  typedef T*         pointer;
  typedef const T*   const_pointer;

  typedef T&         reference;
  typedef const T&   const_reference;

  pointer address( reference r ) const{
    return &r;
  }

  const_pointer address( const_reference r ) const{
    return &r;
  }

  pointer allocate( size_type n, const void* = 0 ){
    pointer p;
    size_t nSize = n * sizeof(T);

    if(CurrentMemoryContext)
      p = (pointer) palloc(nSize);
    else
      p = (pointer) malloc(nSize);

    return p;
  }

  void deallocate( pointer p, size_type n){

    if(CurrentMemoryContext)
      pfree(p);
    else
      free(p);

  }

  void construct( pointer p, const T& val ){
    new (p) T(val);
  }

  void destroy( pointer p ){
    p->~T();
  }

  size_type max_size() const{
    return std::numeric_limits<std::size_t>::max()/sizeof(T);
  }

 private:
  void operator =(const SHMAllocator &);

};


}  // namespace peloton

