//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.h
//
// Identification: src/include/codegen/value.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Mapping of SQL value types to LLVM types. This class helps us generate code
// for common operations on SQL types like comparisons, arithmetic operations
// and boolean operations.
//===----------------------------------------------------------------------===//
class Value {
 public:
  // Constructor that provides the type and the value
  Value();
  Value(type::Type::TypeId type, llvm::Value *value = nullptr,
        llvm::Value *length = nullptr, llvm::Value *null = nullptr);

  // Get the SQL type
  type::Type::TypeId GetType() const { return type_; }

  // Get the LLVM value
  llvm::Value *GetValue() const { return value_; }

  // Get the length of the varchar (if it is one)
  llvm::Value *GetLength() const { return length_; }

  // Return the name of the string
  const std::string GetName() const { return value_->getName(); }

  // Comparison functions
  codegen::Value CompareEq(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareNe(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareLt(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareLte(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareGt(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareGte(CodeGen &codegen, const codegen::Value &o) const;

  // A more exhaustive comparison function between this value and the one
  // provided. We return:
  //  => -1 if this value is less than the other
  //  =>  0 if this value is equal to the other
  //  =>  1 if this value is greater than the other
  codegen::Value CompareForSort(CodeGen &codegen,
                                const codegen::Value &o) const;

  // Mathematical
  codegen::Value Add(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Sub(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Mul(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Div(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Mod(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Min(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Max(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Abs(CodeGen &codegen, const codegen::Value &o) const;

  // Logical/Boolean
  codegen::Value LogicalAnd(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value LogicalOr(CodeGen &codegen, const codegen::Value &o) const;

  // Casting
  codegen::Value CastAs(CodeGen &codegen, type::Type::TypeId type) const;

  // Null checks
  llvm::Value *IsNull(CodeGen &codegen) const;

  // Build a PHI node that combines all the given values (from their basic
  // blocks) into a single value
  static codegen::Value BuildPHI(
      CodeGen &codegen,
      const std::vector<std::pair<codegen::Value, llvm::BasicBlock *>> &vals);

 private:
  friend class Hash;
  friend class CompactStorage;
  friend class UpdateableStorage;

  // Stores the value and type info for two operands after type promotion
  struct OpPromotionMetadata {
    llvm::Value *lhs_val;
    llvm::Value *rhs_val;
    llvm::Type *llvm_type;
    type::Type::TypeId value_type;
  };

  // Generate a hash for the given value
  void ValuesForHash(CodeGen &codegen, llvm::Value *&val,
                     llvm::Value *&len) const;

 public:

  // Return the a representation of this value suitable for materialization
  void ValuesForMaterialization(CodeGen &codegen, llvm::Value *&val,
                                llvm::Value *&len) const;

  // Return a value that can be constructed from the provided type and value
  // registers
  static Value ValueFromMaterialization(CodeGen &codegen, type::Type::TypeId type,
                                        llvm::Value *&val, llvm::Value *&len);
  // Return the types of this value suitable for materialization.
  static void TypeForMaterialization(CodeGen &codegen, type::Type::TypeId type,
                                     llvm::Type *&val_type,
                                     llvm::Type *&len_type);

 private:
  // Create a conditional branch to throw a divide by zero exception
  static void CreateCheckDivideByZeroException(CodeGen &codegen,
                                               const codegen::Value &divisor);

  // Create a conditional branch to throw an overflow exception
  static void CreateCheckOverflowException(CodeGen &codegen,
                                           llvm::Value *overflow_bit);

 public:
  // Get the LLVM type that matches the numeric type provided
  static llvm::Type *NumericType(CodeGen &codegen, type::Type::TypeId type);

  // Return the type promoted LLVM values for the <lhs,rhs> operands
  static OpPromotionMetadata PromoteOperands(CodeGen &codegen, const Value &lhs,
                                             const Value &rhs);

 private:
  // The SQL type
  type::Type::TypeId type_;

  // The value
  llvm::Value *value_;

  // The length of the value (if it's variable in length)
  llvm::Value *length_;

  // If the value is inlined
  bool inlined_;

  // NULL indicator (if any)
  llvm::Value *null_;

  // The (optional) name of this value
  std::string name_;
};

}  // namespace codegen
}  // namespace peloton