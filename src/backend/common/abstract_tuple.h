/**
 * @brief Tuple interface used by the expression system.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/common/types.h"
#include "backend/common/value.h"

namespace nstore {

//===--------------------------------------------------------------------===//
// Tuple Interface
//===--------------------------------------------------------------------===//

class AbstractTuple {
 public:
  virtual ~AbstractTuple() {
  };

  /** @brief Get the value at the given column id. */
  virtual const Value GetValue(oid_t column_id) const = 0;

  /** @brief Get the raw location of the tuple's contents i.e. tuple.value_data. */
  virtual char *GetData() const = 0;

};

} // namespace nstore
