//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_type.h
//
// Identification: src/include/codegen/type/decimal_type.h
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

class Decimal : public SqlType, public Singleton<Decimal> {
 public:
  bool IsVariableLength() const override { return false; }

  Value GetMinValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_MIN);
    return Value{*this, raw_val, nullptr, nullptr};
  }

  Value GetMaxValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_MAX);
    return Value{*this, raw_val, nullptr, nullptr};
  }

  Value GetNullValue(CodeGen &codegen) const override {
    auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_NULL);
    return Value{Type{TypeId(), true}, raw_val, nullptr,
                 codegen.ConstBool(true)};
  }

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override {
    val_type = codegen.DoubleType();
    len_type = nullptr;
  }

  llvm::Function *GetOutputFunction(
      CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const override {
    // TODO: We should be using the precision/scale in the output function
    return ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen);
  }

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  friend class Singleton<Decimal>;

  Decimal();

 private:
  // The boolean type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton