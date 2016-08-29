//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_tuple.h
//
// Identification: src/include/common/abstract_tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/types.h"
#include "common/value.h"

namespace peloton {

namespace catalog {
class Schema;
}

//===--------------------------------------------------------------------===//
// Tuple Interface
//===--------------------------------------------------------------------===//

class AbstractTuple {
 public:
  virtual ~AbstractTuple(){};

  /** @brief Get the value at the given column id. */
  virtual Value GetValue(oid_t column_id) const = 0;

  /** @brief Set the value at the given column id. */
  virtual void SetValue(oid_t column_id, Value &value) = 0;

  /** @brief Get the raw location of the tuple's contents i.e. tuple.value_data.
   */
  virtual char *GetData() const = 0;
};

}  // namespace peloton
