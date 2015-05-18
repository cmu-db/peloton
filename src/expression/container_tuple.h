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

#include "common/types.h"
#include "common/value.h"
#include "expression/tuple.h"
#include "storage/tile_group.h"

namespace nstore {
namespace expression {

template <class T>
class ContainerTuple : public Tuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple& operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple& operator=(ContainerTuple &&) = default;

  ContainerTuple(T *container, oid_t tuple_id)
    : container_(container),
      tuple_id_(tuple_id) {
  }

  const Value GetValue(oid_t column_id) const override {
    assert(container_ != nullptr);
    return container_->GetValue(tuple_id_, column_id);
  }

  inline char *GetData() const override {
    //TODO This is used by tuple address expression. Wtf is that?
    assert(false);
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
