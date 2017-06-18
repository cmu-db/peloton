//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/include/codegen/type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {
namespace type {

class SqlType;

//===----------------------------------------------------------------------===//
// A type represents the runtime type of an attribute. It combines the notions
// of an underlying SQL type and nullability.
//===----------------------------------------------------------------------===//
struct Type {
  // The actual SQL type
  peloton::type::TypeId type_id;

  // Can this type take on NULL?
  bool nullable;

  // Little union to track auxiliary information depending on the type
  union AuxInfo {
    // For numerics, we store the precision and scale
    struct {
      uint32_t precision;
      uint32_t scale;
    } numeric_info;

    // For variable-length types, we store the length here
    uint32_t varlen;
  };

  // Extra type information
  AuxInfo aux_info;

  Type();
  Type(peloton::type::TypeId type_id, bool nullable);
  Type(const SqlType &sql_type, bool nullable = false);

  bool operator==(const Type &other) const;

  const SqlType &GetSqlType() const;

  const TypeSystem &GetTypeSystem() const;

  Type AsNullable() const;

  Type AsNonNullable() const;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton
