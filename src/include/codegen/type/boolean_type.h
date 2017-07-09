//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_type.h
//
// Identification: src/include/codegen/type/boolean_type.h
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

class Boolean : public SqlType, public Singleton<Boolean> {
 public:
  bool IsVariableLength() const override { return false; }

  Value GetMinValue(CodeGen &codegen) const override;

  Value GetMaxValue(CodeGen &codegen) const override;

  Value GetNullValue(CodeGen &codegen) const override;

  void GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                 llvm::Type *&len_type) const override;

  llvm::Function *GetOutputFunction(CodeGen &codegen,
                                    const Type &type) const override;

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
  llvm::Value *Reify(CodeGen &codegen, const codegen::Value &bool_val) const;

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