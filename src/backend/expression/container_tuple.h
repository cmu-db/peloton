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

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/exception.h"
#include "backend/common/abstract_tuple.h"
#include "backend/storage/tile_group.h"

namespace nstore {
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

 private:
  /** @brief Underlying container behind this tuple interface. */
  T *container_;

  /**
   * @brief Tuple id of tuple in tile group that this wrapper is pretending
   *        to be.
   */
  const oid_t tuple_id_;

};

} // namespace expression
} // namespace nstore
