//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varbinary_type.h
//
// Identification: src/include/codegen/type/varbinary_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/type/sql_type.h"
#include "codegen/type/type_system.h"
#include "common/singleton.h"

namespace peloton {
namespace codegen {
namespace type {

class Varbinary : public SqlType, public Singleton<Varbinary> {
 public:
  bool IsVariableLength() const override { return true; }

  Value GetMinValue(CodeGen &codegen) const override;

  Value GetMaxValue(CodeGen &codegen) const override;

  Value GetNullValue(CodeGen &codegen) const override;

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override;

  llvm::Function *GetInputFunction(CodeGen &codegen,
                                   const Type &type) const override;

  llvm::Function *GetOutputFunction(CodeGen &codegen,
                                    const Type &type) const override;

  const TypeSystem &GetTypeSystem() const override { return type_system_; }

 private:
  friend class Singleton<Varbinary>;

  Varbinary();

 private:
  // The blob type's type-system
  TypeSystem type_system_;
};

}  // namespace type
}  // namespace codegen
}  // namespace peloton