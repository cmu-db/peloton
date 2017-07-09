//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.cpp
//
// Identification: src/codegen/type/type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/type.h"

#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {
namespace type {

Type::Type() : Type(peloton::type::TypeId::INVALID, false) {}

Type::Type(peloton::type::TypeId type_id, bool _nullable)
    : type_id(type_id), nullable(_nullable) {}

Type::Type(const SqlType &sql_type, bool _nullable)
    : Type(sql_type.TypeId(), _nullable) {}

bool Type::operator==(const Type &other) const {
  return type_id == other.type_id;
}

const SqlType &Type::GetSqlType() const { return SqlType::LookupType(type_id); }

const TypeSystem &Type::GetTypeSystem() const {
  return GetSqlType().GetTypeSystem();
}

Type Type::AsNullable() const {
  if (nullable) {
    return *this;
  }

  Type copy = *this;
  copy.nullable = true;
  return copy;
}

Type Type::AsNonNullable() const {
  if (!nullable) {
    return *this;
  }

  Type copy = *this;
  copy.nullable = false;
  return copy;
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
