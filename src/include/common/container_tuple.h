//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// container_tuple.h
//
// Identification: src/include/common/container_tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <functional>
#include <vector>

#include "common/types.h"
#include "common/value.h"
#include "common/macros.h"
#include "common/exception.h"
#include "common/abstract_tuple.h"
#include "storage/tile_group.h"
#include "catalog/schema.h"

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

  ContainerTuple(T *container, oid_t tuple_id,
                 const std::vector<oid_t> *column_ids)
      : container_(container), tuple_id_(tuple_id), column_ids_(column_ids) {}

  /* Accessors */
  T *GetContainer() const { return container_; }

  oid_t GetTupleId() const { return tuple_id_; }

  void SetValue(UNUSED_ATTRIBUTE oid_t column_id,
                UNUSED_ATTRIBUTE const common::Value &value) {
  }

  /** @brief Get the value at the given column id. */
  common::Value GetValue(oid_t column_id) const override {
    PL_ASSERT(container_ != nullptr);

    return container_->GetValue(tuple_id_, column_id);
  }

  /** @brief Get the raw location of the tuple's contents. */
  inline char *GetData() const override {
    // NOTE: We can't.Get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException(
        "GetData() not supported for container tuples.");
    return nullptr;
  }

  /** @brief Compute the hash value based on all valid columns and a given seed.
   */
  size_t HashCode(size_t seed = 0) const {
    if (column_ids_) {
      for (auto &column_itr : *column_ids_) {
        common::Value value = GetValue(column_itr);
        value.HashCombine(seed);
      }
    } else {
      oid_t column_count = container_->GetColumnCount();
      for (size_t column_itr = 0; column_itr < column_count; column_itr++) {
        common::Value value = GetValue(column_itr);
        value.HashCombine(seed);
      }
    }
    return seed;
  }

  /** @brief Compare whether this tuple equals to other value-wise.
   * Assume the schema of other tuple.Is the same as this. No check.
   */
  bool EqualsNoSchemaCheck(const ContainerTuple<T> &other) const {
    if (column_ids_) {
      for (auto &column_itr : *column_ids_) {
        common::Value lhs = (GetValue(column_itr));
        common::Value rhs = (other.GetValue(column_itr));
        common::Value cmp = (lhs.CompareNotEquals(rhs));
        if (cmp.IsTrue()) {
          return false;
        }
      }
    } else {
      oid_t column_count = container_->GetColumnCount();
      for (size_t column_itr = 0; column_itr < column_count; column_itr++) {
        common::Value lhs = (GetValue(column_itr));
        common::Value rhs = (other.GetValue(column_itr));
        common::Value cmp = (lhs.CompareNotEquals(rhs));
        if (cmp.IsTrue())
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

  /** @brief The ids of column that this tuple cares about
   *  This enables this class only looks at a subset of a tuple
   * */
  const std::vector<oid_t> *column_ids_ = nullptr;
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
// Specialization for std::vector<common::Value>
//===--------------------------------------------------------------------===//
/**
 * @brief A convenient wrapper to interpret a vector of values as an tuple.
 * No need to construct a schema.
 * The caller should make sure there's no out-of-bound calls.
 */
template <>
class ContainerTuple<std::vector<common::Value>> : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(std::vector<common::Value> *container) : container_(container) {}

  /** @brief Get the value at the given column id. */
  common::Value GetValue(oid_t column_id) const override {
    PL_ASSERT(container_ != nullptr);
    PL_ASSERT(column_id < container_->size());

    return ((*container_)[column_id]);
  }

  void SetValue(UNUSED_ATTRIBUTE oid_t column_id,
    UNUSED_ATTRIBUTE const common::Value &value) {}

  /** @brief Get the raw location of the tuple's contents. */
  inline char *GetData() const override {
    // NOTE: We can't.Get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException(
        "GetData() not supported for container tuples.");
    return nullptr;
  }

  size_t HashCode(size_t seed = 0) const {
    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      const common::Value value = GetValue(column_itr);
      value.HashCombine(seed);
    }
    return seed;
  }

  /** @brief Compare whether this tuple equals to other value-wise.
   * Assume the schema of other tuple.Is the same as this. No check.
   */
  bool EqualsNoSchemaCheck(
      const ContainerTuple<std::vector<common::Value>> &other) const {
    PL_ASSERT(container_->size() == other.container_->size());

    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      common::Value lhs = GetValue(column_itr);
      common::Value rhs = other.GetValue(column_itr);
      common::Value cmp = lhs.CompareNotEquals(rhs);
      if (cmp.IsTrue())
        return false;
    }
    return true;
  }

 private:
  const std::vector<common::Value > *container_ = nullptr;
};

template<>
class ContainerTuple<storage::TileGroup> : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(storage::TileGroup *container, oid_t tuple_id)
      : container_(container), tuple_id_(tuple_id) {}

  ContainerTuple(storage::TileGroup *container, oid_t tuple_id,
                 const std::vector<oid_t> *column_ids)
      : container_(container), tuple_id_(tuple_id), column_ids_(column_ids) {}

  /* Accessors */
  storage::TileGroup *GetContainer() const { return container_; }

  oid_t GetTupleId() const { return tuple_id_; }

  /** @brief Get the value at the given column id. */
  common::Value GetValue(oid_t column_id) const override {
    PL_ASSERT(container_ != nullptr);

    return container_->GetValue(tuple_id_, column_id);
  }

  void SetValue(oid_t column_id, const common::Value &value) {
    common::Value val = value.Copy();
    container_->SetValue(val, tuple_id_, column_id);
  }

  inline char *GetData() const override {
    // NOTE: We can't.Get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
    throw NotImplementedException(
        "GetData() not supported for container tuples.");
    return nullptr;
  }

 private:
  /** @brief Underlying container behind this tuple interface. */
  storage::TileGroup *container_;

  /**
   * @brief Tuple id of tuple in tile group that this wrapper is pretending
   *        to be.
   */
  const oid_t tuple_id_;

  /** @brief The ids of column that this tuple cares about
   *  This enables this class only looks at a subset of a tuple
   * */
  const std::vector<oid_t> *column_ids_ = nullptr;
};

}  // End expression namespace
}  // End peloton namespace
