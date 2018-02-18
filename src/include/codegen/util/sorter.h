//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.h
//
// Identification: src/include/codegen/util/sorter.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>

#include "executor/executor_context.h"

namespace peloton {
namespace codegen {
namespace util {

/**
 * Sorters are a class than enable the storage and sorting of arbitrary sized
 * tuples. This class is meant to mimic a std::vector<T> providing contiguous
 * storage space for elements of type T. The main, and most important,
 * difference to std::vector<T> is that the size of T is not a compile-time
 * constant, but is known only at runtime **after** an instance of this class
 * has been instantiated. For this reason, we can't put templates on the class.
 *
 * This class only supports forward iteration since this is all that's required.
 * Additionally, Sorter does not serialize elements into its memory space.
 * Instead, it allocates space for incoming tuples on demand and returns a
 * pointer to the call, relying on her to serialize into the space. This assumes
 * well-behaved callers. We **could** accept a Serializer type as part of
 * Init(..), but we don't need it at this moment.
 */
class Sorter {
 private:
  // We allocate 4KB of buffer space upon initialization
  static constexpr uint64_t kInitialBufferSize = 4 * 1024;

 public:
  typedef int (*ComparisonFunction)(const char *left_tuple,
                                    const char *right_tuple);

  /// Constructor
  Sorter(ComparisonFunction func, uint32_t tuple_size);

  /// Destructor
  ~Sorter();

  /**
   * @brief This static function initializes the given sorter instance with the
   * given comparison function and assumes all input tuples have the given size.
   *
   * @param sorter The sorter instance we are initializing
   * @param func The comparison function used during sort
   * @param tuple_size The size of the tuple in bytes
   */
  static void Init(Sorter &sorter, ComparisonFunction func,
                   uint32_t tuple_size);

  /**
   * @brief Cleans up all resources maintained by the given sorter instance
   */
  static void Destroy(Sorter &sorter);

  /**
   * @brief Allocate space for a new input tuple in this sorter. It is assumed
   * that the size of the tuple is equivalent to the tuple size provided when
   * this sorter was initialized.
   *
   * @return A pointer to a memory space large enough to store one tuple
   */
  char *StoreInputTuple();

  /**
   * @brief Sort all tuples stored in this sorter.
   */
  void Sort();

  /**
   * @brief Perform a parallel sort of all sorter instances stored in the thread
   * states object.
   */
  void SortParallel(
      const executor::ExecutorContext::ThreadStates &thread_states,
      uint32_t sorter_offset);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Return the number of tuples
  uint64_t NumTuples() const { return tuples_.size(); }

  /// Iterators
  std::vector<char *>::iterator begin() { return tuples_.begin(); }
  std::vector<char *>::iterator end() { return tuples_.end(); }

 private:
  void MakeRoomForNewTuple();

 private:
  // The comparison function
  ComparisonFunction cmp_func_;

  // The size of the tuples
  uint32_t tuple_size_;

  // The following two pointers are pointers into the currently active block
  // buffer_pos_ is the position in the block the next tuple will be written to.
  // buffer_end_ marks the end of the block
  char *buffer_pos_;
  char *buffer_end_;

  // Sorters double-expand
  uint64_t next_alloc_size_;

  // The tuples
  std::vector<char *> tuples_;
  char **tuples_start_;
  char **tuples_end_;

  // The memory blocks we've allocated and their sizes
  std::vector<std::pair<void *, uint64_t>> blocks_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton