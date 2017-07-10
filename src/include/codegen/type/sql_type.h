//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sql_type.h
//
// Identification: src/include/codegen/type/sql_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "type/type_id.h"

namespace llvm {
class Type;
class Function;
}  // namespace llvm

namespace peloton {
namespace codegen {

// Forward declare
class CodeGen;
class Value;

namespace type {

// Forward declare
class Type;
class TypeSystem;

//===----------------------------------------------------------------------===//
// Each SQL data type supported by Peloton implements this interface. Examples
// are tinyint, smallint, integer, bigint, decimal, varchar etc. This interface
// provides generic information such as the storage size, the minimum and
// maximum values, and the underlying physical storage format. Most importantly,
// every SQL data type has a type system that encapsulates the behaviour of- and
// operations on- this SQL data type.
//===----------------------------------------------------------------------===//
class SqlType {
 public:
  SqlType(peloton::type::TypeId type_id) : type_id_(type_id) {}
  virtual ~SqlType() {}

  virtual peloton::type::TypeId TypeId() const { return type_id_; }

  virtual bool IsVariableLength() const = 0;
  virtual Value GetMinValue(CodeGen &codegen) const = 0;
  virtual Value GetMaxValue(CodeGen &codegen) const = 0;
  virtual Value GetNullValue(CodeGen &codegen) const = 0;
  virtual void GetTypeForMaterialization(CodeGen &codegen,
                                         llvm::Type *&val_type,
                                         llvm::Type *&len_type) const = 0;
  virtual llvm::Function *GetOutputFunction(CodeGen &codegen,
                                            const Type &type) const = 0;
  virtual const TypeSystem &GetTypeSystem() const = 0;

  // Given a type ID, get the SQL Type instance
  static const SqlType &LookupType(peloton::type::TypeId type_id);

  // Equality
  bool operator==(const SqlType &o) const { return TypeId() == o.TypeId(); }
  bool operator!=(const SqlType &o) const { return !(*this == o); }

 private:
  // The unique ID of this type
  peloton::type::TypeId type_id_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton
