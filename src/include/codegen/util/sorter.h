//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.h
//
// Identification: src/include/codegen/util/sorter.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

namespace peloton {
namespace codegen {
namespace util {

//===----------------------------------------------------------------------===//
// A class than enables the storage and sorting of arbitrarily sized tuples.
// Note that this class is meant to mimic a std::vector<T> providing
// contiguous storage space for elements of type T.  However, there are a few
// differences:
//
// 1) This class is called from LLVM code, hence we do not want to worry about
//    defining types for std::vector.
// 2) The _type_ of element it will store (and hence its size) is only be known
//    at run-time (i.e., _after_ an instance has been instantiated). Therefore,
//    we can't put templates on this guy.
// 3) Post-sort, we only need sequential access to elements in the sorted
//    buffer. This is why we only provide a single uni-directional iterator.
// 4) Unlike std::vector, this class will (lazily) allocate space for incoming
//    tuples and let clients worry about serializing types into the allocated
//    space. We would accept a Serializer type as part of the Init(..) function,
//    but we don't need it at this moment.
//===----------------------------------------------------------------------===//
class Sorter {
 private:
  // We (arbitrarily) allocate 4MB of buffer space upon initialization
  static constexpr uint64_t kInitialBufferSize = 1 * 1024 * 1024 * 4;

 public:
  typedef int (*ComparisonFunction)(const void *left_tuple,
                                    const void *right_tuple);

  // Constructor
  Sorter();
  // Destructor
  ~Sorter();

  // Initialize this sorter with the given comparison function
  void Init(ComparisonFunction func, uint32_t tuple_size);

  // StoreValue an input tuple whose size is _equivalent_ to the size of tuple
  // provided at initialization time.
  char *StoreInputTuple();

  // Perform the sort
  void Sort();

  // Cleanup all the resources this sorter maintains
  void Destroy();

  // Iterator over the tuples in the sort
  struct Iterator {
   public:
    // Move the iterator forward
    Iterator &operator++();
    // (In)Equality check
    bool operator==(const Iterator &rhs) const;
    bool operator!=(const Iterator &rhs) const;
    // Dereference
    const char *operator*();

   private:
    friend class Sorter;
    // Private constructor so only the sorter can create these
    Iterator(char *pos, uint32_t tuple_size)
        : curr_pos_(pos), tuple_size_(tuple_size) {}

    // Private position
    char *curr_pos_;
    uint32_t tuple_size_;
  };

  // Iterators
  Iterator begin();
  Iterator end();

  uint64_t GetNumTuples() const { return GetUsedSpace() / tuple_size_; }

 private:
  // Is there enough room in the buffer to store a tuple of the provided size?
  bool EnoughSpace(uint32_t tuple_size) const {
    return buffer_start_ != nullptr && buffer_pos_ + tuple_size < buffer_end_;
  }

  uint64_t GetAllocatedSpace() const { return buffer_end_ - buffer_start_; }
  uint64_t GetUsedSpace() const { return buffer_pos_ - buffer_start_; }

  // Resize the given array to a larger size
  void Resize();

 private:
  // The contiguous buffer space where tuples are stored.
  //
  // The invariant this class holds for the following buffer pointers are:
  //   1) buffer_start_ is NULL, in which case both buffer_pos_ and buffer_end_
  //    are also NULL.
  // OR
  //   2) buffer_start <= buffer_pos < buffer_end
  //
  char *buffer_start_;
  char *buffer_pos_;
  char *buffer_end_;

  // The size of the tuples
  uint32_t tuple_size_;

  // The comparison function
  ComparisonFunction cmp_func_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton