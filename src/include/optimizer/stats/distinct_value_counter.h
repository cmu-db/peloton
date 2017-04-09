#pragma once

#include "type/value.h"
#include "type/type.h"

namespace peloton {
namespace optimizer {

/*
 * DistinctValueCounter - interface for counting number of distinct values.
 *
 * All algorithms for counting distinct values must implement this interface.
 */
class DistinctValueCounter {
public:
  DistinctValueCounter() {}

  virtual ~DistinctValueCounter() {}

  virtual void AddValue(type::Value &value) = 0;

  virtual double EstimateCardinality() = 0;

  virtual void Clear() = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
