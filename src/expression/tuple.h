/**
 * @brief Tuple interface used by the expression system.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "common/value.h"

namespace nstore {
namespace expression {

class Tuple {
 public:
  virtual ~Tuple() {
  };

  virtual const Value GetValue(oid_t column_id) const = 0;

  virtual char *GetData() const = 0;
};

} // namespace expression
} // namespace nstore
