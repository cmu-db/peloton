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
#include "common/singleton.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

class Boolean : public SqlType, public Singleton<Boolean> {
 public:
  bool IsVariableLength() const override { return false; }

  Value GetMinValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_MIN);
    return Value{*this, raw_val, nullptr, nullptr};
  }

  Value GetMaxValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_MAX);
    return Value{*this, raw_val, nullptr, nullptr};
  }

  Value GetNullValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_NULL);
    return Value{Type{TypeId(), true}, raw_val, nullptr,
                 codegen.ConstBool(true)};
  }

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override {
    val_type = codegen.BoolType();
    len_type = nullptr;
  }

  llvm::Function *GetOutputFunction(
      CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const override {
    return ValuesRuntimeProxy::_OutputBoolean::GetFunction(codegen);
  }

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

  // This method reifies a NULL-able boolean value, thanks to the weird-ass
  // three-valued logic in SQL. The logic adheres to the table below:
  //
  //   INPUT | OUTPUT
  // +-------+--------+
  // | false | false  |
  // +-------+--------+
  // | null  | false  |
  // +-------+--------+
  // | true  | true   |
  // +-------+--------+
  //
  llvm::Value *Reify(CodeGen &codegen, const Value &bool_val) const {
    if (!bool_val.IsNullable()) {
      return bool_val.GetValue();
    } else {
      return codegen->CreateSelect(bool_val.IsNull(codegen),
                                   codegen.ConstBool(false),
                                   bool_val.GetValue());
    }
  }

 private:
  friend class Singleton<Boolean>;

  Boolean();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton