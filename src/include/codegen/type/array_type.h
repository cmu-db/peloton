//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// array_type.h
//
// Identification: src/include/codegen/type/array_type.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/value.h"
#include "codegen/values_runtime_proxy.h"
#include "codegen/type/sql_type.h"
#include "codegen/type/type_system.h"
#include "common/exception.h"
#include "common/singleton.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

class Array : public SqlType, public Singleton<Array> {
 public:
  bool IsVariableLength() const override { return true; }

  Value GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"Arrays don't have minimum values ...."};
  }

  Value GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"Arrays don't have maximum values ...."};
  }

  Value GetNullValue(CodeGen &codegen) const override {
    return Value{*this, codegen.NullPtr(codegen.CharPtrType()),
                 codegen.Const32(0), codegen.ConstBool(true)};
  }

  void GetTypeForMaterialization(
      UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Type *&val_type,
      UNUSED_ATTRIBUTE llvm::Type *&len_type) const override {
    // TODO
    throw NotImplementedException{
        "Arrays currently do not have a materialization format. Fix me."};
  }

  llvm::Function *GetOutputFunction(
      UNUSED_ATTRIBUTE CodeGen &codegen,
      UNUSED_ATTRIBUTE const Type &type) const override {
    throw NotImplementedException{"Array's can't be output ... for now ..."};
  }

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  friend class Singleton<Array>;

  Array();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton