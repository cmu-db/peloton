//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// varlen.h
//
// Identification: src/backend/common/varlen.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>

namespace peloton {

class VarlenPool;

//===--------------------------------------------------------------------===//
// Storage space for variable length fields
//===--------------------------------------------------------------------===//

/**
 * An object to use in lieu of raw char* pointers for strings
 * which are not inlined into tuple storage. This provides a
 * constant value to live in tuple storage while allowing the memory
 * containing the actual string to be moved around as the result of
 * compaction.
 */
class Varlen {
 public:
  /// Create and return a new Varlen object which points to an
  /// allocated memory block of the requested size. The caller
  /// may provide an optional Pool from which the memory (and
  /// the memory for the Varlen object itself) will be
  /// allocated, intended for temporary strings. If no Pool
  /// object is provided, the Varlen and the string memory will be
  /// allocated out of the ThreadLocalPool.
  static Varlen *Create(std::size_t size, VarlenPool *data_pool = NULL);

  /// Destroy the given Varlen object and free any memory, if
  /// any, allocated from pools to store the object.
  /// varlen must have been allocated and returned by a call to
  /// Varlen::create() and must not have been created in a
  /// temporary Pool
  static void Destroy(Varlen *varlen);

  /**
   * @brief Clone (deep copy) the source Varlen in the provided data pool.
   */
  static Varlen *Clone(const Varlen &src, VarlenPool *data_pool = NULL);

  char *Get();
  const char *Get() const;

 private:
  Varlen(std::size_t size);
  Varlen(std::size_t size, VarlenPool *data_pool);
  ~Varlen();

  /// Callback used via the back-pointer in order to update the
  /// pointer to the memory backing this string reference
  void UpdateStringLocation(void *location);

  void SetBackPtr();

  std::size_t varlen_size;

  bool varlen_temp_pool;

  char *varlen_string_ptr;
};

}  // End peloton namespace
