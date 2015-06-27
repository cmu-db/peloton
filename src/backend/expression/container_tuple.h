/**
 * @brief Implementation of the tuple interface that wraps a container
 *        such as a tile group or logical tile, allowing it to be used
 *        with the expression system without materializing a tuple as an
 *        intermediate step.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <cassert>
#include <functional>

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/exception.h"
#include "backend/common/abstract_tuple.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Container Tuple wrapping a tile group or logical tile.
//===--------------------------------------------------------------------===//

template <class T>
class ContainerTuple : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple& operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple& operator=(ContainerTuple &&) = default;

  ContainerTuple(T *container, oid_t tuple_id)
  : container_(container),
    tuple_id_(tuple_id) {
  }

  /** @brief Get the value at the given column id. */
  const Value GetValue(oid_t column_id) const override {
    assert(container_ != nullptr);

    return container_->GetValue(tuple_id_, column_id);
  }

  /** @brief Get the raw location of the tuple's contents. */
  inline char *GetData() const override{
    // NOTE: We can't get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException("GetData() not supported for container tuples.");
    return nullptr;
  }

  /** @brief Compute the hash value based on all valid columns and a given seed. */
  size_t HashCode(size_t seed = 0) const {
    const int column_count = container_->GetColumnCount();

    for (int column_itr = 0; column_itr < column_count; column_itr++) {
      const Value value = GetValue(column_itr);
      value.HashCombine(seed);
    }
    return seed;
  }

  /** @brief Compare whether this tuple equals to other value-wise.
   * Assume the schema of other tuple is the same as this. No check.
   */
  bool EqualsNoSchemaCheck(const ContainerTuple<T> &other) const {
    const int column_count = container_->GetColumnCount();

    for (int column_itr = 0; column_itr < column_count; column_itr++) {
      const Value lhs = GetValue(column_itr);
      const Value rhs = other.GetValue(column_itr);
      if (lhs.OpNotEquals(rhs).IsTrue()) {
        return false;
      }
    }
    return true;
  }

 private:
  /** @brief Underlying container behind this tuple interface. */
  T *container_;

  /**
   * @brief Tuple id of tuple in tile group that this wrapper is pretending
   *        to be.
   */
  const oid_t tuple_id_;

};

template <class T>
struct ContainerTupleHasher: std::unary_function<ContainerTuple<T>, std::size_t> {
  // Generate a 64-bit number for the key value
  size_t operator()(ContainerTuple<T> tuple) const {
    return tuple.HashCode();
  }
};

template <class T>
class ContainerTupleComparator {
 public:
  bool operator()(const ContainerTuple<T> lhs, const ContainerTuple<T> rhs) const {
    return lhs.EqualsNoSchemaCheck(rhs);
  }
};

} // namespace expression
} // namespace peloton
