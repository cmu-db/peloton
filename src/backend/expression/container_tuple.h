//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// container_tuple.h
//
// Identification: src/backend/expression/container_tuple.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <functional>
#include <vector>

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
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(T *container, oid_t tuple_id)
      : container_(container), tuple_id_(tuple_id) {}

  /* Accessors */
  T *GetContainer() const { return container_; }

  oid_t GetTupleId() const { return tuple_id_; }

  /** @brief Get the value at the given column id. */
  const Value GetValue(oid_t column_id) const override {
    assert(container_ != nullptr);

    return container_->GetValue(tuple_id_, column_id);
  }

  /** @brief Get the raw location of the tuple's contents. */
  inline char *GetData() const override {
    // NOTE: We can't get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException(
        "GetData() not supported for container tuples.");
    return nullptr;
  }

  /** @brief Compute the hash value based on all valid columns and a given seed.
   */
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

//===--------------------------------------------------------------------===//
// ContainerTuple Hasher
//===--------------------------------------------------------------------===//
template <class T>
struct ContainerTupleHasher
    : std::unary_function<ContainerTuple<T>, std::size_t> {
  // Generate a 64-bit number for the key value
  size_t operator()(const ContainerTuple<T> &tuple) const {
    return tuple.HashCode();
  }
};

//===--------------------------------------------------------------------===//
// ContainerTuple Comparator
//===--------------------------------------------------------------------===//
template <class T>
class ContainerTupleComparator {
 public:
  bool operator()(const ContainerTuple<T> &lhs,
                  const ContainerTuple<T> &rhs) const {
    return lhs.EqualsNoSchemaCheck(rhs);
  }
};

//===--------------------------------------------------------------------===//
// Specialization for std::vector<Value>
//===--------------------------------------------------------------------===//
/**
 * @brief A convenient wrapper to interpret a vector of values as an tuple.
 * No need to construct a schema.
 * The caller should make sure there's no out-of-bound calls.
 */
template<>
class ContainerTuple< std::vector<Value> > : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(std::vector<Value>* container)
    : container_(container){
  }

  /** @brief Get the value at the given column id. */
  const Value GetValue(oid_t column_id) const override {
    assert(container_ != nullptr);
    assert(column_id < container_->size());

    return (*container_)[column_id];
  }

  /** @brief Get the raw location of the tuple's contents. */
  inline char *GetData() const override {
    // NOTE: We can't get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException(
        "GetData() not supported for container tuples.");
    return nullptr;
  }

  size_t HashCode(size_t seed = 0) const {

    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      const Value value = GetValue(column_itr);
      value.HashCombine(seed);
    }
    return seed;
  }

  /** @brief Compare whether this tuple equals to other value-wise.
   * Assume the schema of other tuple is the same as this. No check.
   */
  bool EqualsNoSchemaCheck(const ContainerTuple< std::vector<Value> > &other) const {
    assert(container_->size() == other.container_->size());

    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      const Value lhs = GetValue(column_itr);
      const Value rhs = other.GetValue(column_itr);
      if (lhs.OpNotEquals(rhs).IsTrue()) {
        return false;
      }
    }
    return true;
  }

 private:
  const std::vector<Value>* container_ = nullptr;
};

}  // End expression namespace
}  // End peloton namespace
