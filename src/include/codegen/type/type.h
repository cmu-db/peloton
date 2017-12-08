//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.h
//
// Identification: src/include/codegen/type/type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "type/type_id.h"

namespace peloton {
namespace codegen {
namespace type {

class SqlType;
class TypeSystem;

//===----------------------------------------------------------------------===//
// A type represents the runtime type of an attribute. It combines the notions
// of an underlying SQL type and nullability. The codegen component uses this
// compile-time information to optimize the code-generation process.
//
// Types also carry SQL-type-specific information in the AuxInfo member struct.
// For example, DECIMAL/NUMERIC types also carry the precision and scale of the
// number at runtime. For variable-length fields, it will carry the length of
// the data element. This information is needed during code generation.
//===----------------------------------------------------------------------===//
class Type {
 public:
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

  // Simple constructors
  Type();
  Type(peloton::type::TypeId type_id, bool nullable);
  Type(const SqlType &sql_type, bool nullable = false);

  // Equality check
  bool operator==(const Type &other) const;
  bool operator!=(const Type &other) const { return !(*this == other); }

  // Get the SQL type of this runtime type
  const SqlType &GetSqlType() const;

  // Get the SQL type-system of the SQL type of this runtime type
  const TypeSystem &GetTypeSystem() const;

  // Get this type, but as NULL-able
  Type AsNullable() const;

  // Get this type, but as non-NULL-able
  Type AsNonNullable() const;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton
