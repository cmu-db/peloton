//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.h
//
// Identification: src/include/codegen/util/sorter.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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
 * pointer to the call, relying on her to serialize into the space.
 */
class Sorter {
 private:
  // We allocate 4KB of buffer space upon initialization
  static constexpr uint64_t kInitialBufferSize = 4 * 1024;

 public:
  using ComparisonFunction = int (*)(const char *left_tuple,
                                     const char *right_tuple);

  /**
   * Constructor to create and setup this sorter instance.
   *
   * TODO(pmenon): Right now, Sorters assume that all tuples that are inserted
   * have the same size. If we choose to perform null-suppression or any
   * compression, this is no longer true.
   *
   * @param memory A memory pool that is used for all allocations during the
   * lifetime of the sorter.
   * @param func The comparison function used to compare two tuples stored in
   * this sorter
   * @param tuple_size The size of the tuples stored in this sorter
   */
  Sorter(::peloton::type::AbstractPool &memory, ComparisonFunction func,
         uint32_t tuple_size);

  /**
   * Destructor. This destructor cleans up returns all memory it has allocated
   * back to the memory pool that injected at construction.
   */
  ~Sorter();

  /**
   * This static function initializes the given sorter instance with the given
   * comparison function and assumes all input tuples have the given size. This
   * method is used from codegen to invoke the constructor of the sorter
   * sorter instance.
   *
   * @param sorter The sorter instance we are initializing
   * @param func The comparison function used during sort
   * @param tuple_size The size of the tuple in bytes
   */
  static void Init(Sorter &sorter, executor::ExecutorContext &ctx,
                   ComparisonFunction func, uint32_t tuple_size);

  /**
   * Cleans up all resources maintained by the given sorter instance. This
   * method is used from codegen to invoke the destructor of a sorter instance.
   *
   * @param sorter The sorter instance we're destroying the resources from
   */
  static void Destroy(Sorter &sorter);

  /**
   * Allocate space for a new input tuple in this sorter. It is assumed that the
   * size of the tuple is equivalent to the tuple size provided when this sorter
   * was initialized.
   *
   * @return A pointer to a memory space large enough to store one tuple
   */
  char *StoreInputTuple();

  /**
   * Sort all tuples stored in this sorter instance. This is a single-threaded
   * synchronous call.
   */
  void Sort();

  /**
   * Perform a parallel sort of all sorter instances stored in the thread states
   * object. Each thread-local sorter instance is unsorted.
   *
   * @param thread_states The states object where all the sorter instances are
   * stored.
   * @param sorter_offset The offset in each thread state where the sorters are
   * stored.
   */
  void SortParallel(
      const executor::ExecutorContext::ThreadStates &thread_states,
      uint32_t sorter_offset);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /** Return the number tuples stored in this sorter instance */
  uint64_t NumTuples() const { return tuples_.size(); }

  /** Iterators */
  std::vector<char *>::iterator begin() { return tuples_.begin(); }
  std::vector<char *>::iterator end() { return tuples_.end(); }

 private:
  /**
   * Allocate room for a new tuple. If room is already available, return
   * immediately. If room has to be made, allocate a block of memory from the
   * memory pool.
   */
  void MakeRoomForNewTuple();

  /**
   * Transfer ownership of all allocated memory to the provided sorter instance.
   *
   * @param target The sorter instance that is accepting custody of the memory
   * we've allocated.
   */
  void TransferMemoryBlocks(Sorter &target);

 private:
  // The memory pool where this sorter sources memory from
  ::peloton::type::AbstractPool &memory_;

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