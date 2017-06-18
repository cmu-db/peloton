//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_type.h
//
// Identification: src/include/codegen/type/boolean_type.h
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

class Varchar : public SqlType, public Singleton<Varchar> {
 public:
  bool IsVariableLength() const override { return true; }

  Value GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"Varchars don't have minimum values ...."};
  }

  Value GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const override {
    throw Exception{"Varchars don't have maximum values ...."};
  }

  Value GetNullValue(CodeGen &codegen) const override {
    return Value{Type{TypeId(), true}, codegen.NullPtr(codegen.CharPtrType()),
                 codegen.Const32(0), codegen.ConstBool(true)};
  }

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override {
    val_type = codegen.CharPtrType();
    len_type = codegen.Int32Type();
  }

  llvm::Function *GetOutputFunction(
      CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const override {
    // TODO: We should use the length information in the type?
    return ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen);
  }

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  friend class Singleton<Varchar>;

  Varchar();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton