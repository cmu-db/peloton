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

#include "common/printable.h"
#include "internal_types.h"
#include "type/value.h"

namespace peloton {

namespace catalog {
class Schema;
}

//===--------------------------------------------------------------------===//
// Tuple Interface
//===--------------------------------------------------------------------===//

class AbstractTuple : public Printable {
 public:
  virtual ~AbstractTuple(){};

  /** @brief Get the value at the given column id. */
  virtual type::Value GetValue(oid_t column_id) const = 0;

  /** @brief Set the value at the given column id. */
  virtual void SetValue(oid_t column_id, const type::Value &value) = 0;

  /** @brief Get the raw location of the tuple's contents i.e. tuple.value_data.
   */
  virtual char *GetData() const = 0;
};

}  // namespace peloton
