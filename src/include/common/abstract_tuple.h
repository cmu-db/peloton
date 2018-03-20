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

//===----------------------------------------------------------------------===//
// Generic tuple interface
//===----------------------------------------------------------------------===//
class AbstractTuple : public Printable {
 public:
  virtual ~AbstractTuple() = default;

  /**
   * @brief Get the value at the given column id
   *
   * @param column_id The ID/offset of the column whose value to return
   */
  virtual type::Value GetValue(oid_t column_id) const = 0;

  /**
   * @brief Set the value at the given column id
   *
   * @param column_id The ID/offset of the column in the tuple to set
   * @param value The value to set the column to
   **/
  virtual void SetValue(oid_t column_id, const type::Value &value) = 0;

  /**
   * @brief Get the raw location of the tuple's contents i.e. tuple.value_data.
   */
  virtual char *GetData() const = 0;
};

}  // namespace peloton
