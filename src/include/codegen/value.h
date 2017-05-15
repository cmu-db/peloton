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
        llvm::Value *length = nullptr, llvm::Value *is_null = nullptr);

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Get the SQL type
  type::Type::TypeId GetType() const { return type_; }

  // Get the LLVM value
  llvm::Value *GetValue() const { return value_; }

  // Get the length of the varchar (if it is one)
  llvm::Value *GetLength() const { return length_; }

  // Get the null indicator value
  llvm::Value *GetNullBit() const { return null_; }

  // Return the name of the string
  const std::string GetName() const { return value_->getName(); }

  // Reify this (potentially null) value into a boolean value
  llvm::Value *ReifyBoolean(CodeGen &codegen) const;

  // Nullability checks
  llvm::Value *IsNull(CodeGen &codegen) const;
  llvm::Value *IsNotNull(CodeGen &codegen) const;

  //===--------------------------------------------------------------------===//
  // Comparison functions
  //===--------------------------------------------------------------------===//
  codegen::Value CastTo(CodeGen &codegen, type::Type::TypeId to_type) const;

  codegen::Value CompareEq(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareNe(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareLt(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareLte(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareGt(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value CompareGte(CodeGen &codegen, const codegen::Value &o) const;

  static codegen::Value TestEquality(CodeGen &codegen,
                                     const std::vector<codegen::Value> &lhs,
                                     const std::vector<codegen::Value> &rhs);

  // Perform a comparison used for sorting. We need a stable and transitive
  // sorting comparison function here. The function returns:
  //  < 0 - if the left value comes before the right value when sorted
  //  = 0 - if the left value is equivalent to the right element
  //  > 0 - if the left value comes after the right value when sorted
  codegen::Value CompareForSort(CodeGen &codegen,
                                const codegen::Value &o) const;

  //===--------------------------------------------------------------------===//
  // Mathematical functions
  //===--------------------------------------------------------------------===//

  enum class OnError : uint32_t { ReturnDefault, ReturnNull, Exception };

  codegen::Value Add(CodeGen &codegen, const codegen::Value &right,
                     const OnError on_error = OnError::Exception) const;
  codegen::Value Sub(CodeGen &codegen, const codegen::Value &right,
                     const OnError on_error = OnError::Exception) const;
  codegen::Value Mul(CodeGen &codegen, const codegen::Value &right,
                     const OnError on_error = OnError::Exception) const;
  codegen::Value Div(CodeGen &codegen, const codegen::Value &right,
                     const OnError on_error = OnError::Exception) const;
  codegen::Value Mod(CodeGen &codegen, const codegen::Value &right,
                     const OnError on_error = OnError::Exception) const;
  codegen::Value Min(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value Max(CodeGen &codegen, const codegen::Value &o) const;

  //===--------------------------------------------------------------------===//
  // Logical/Boolean functions
  //===--------------------------------------------------------------------===//

  codegen::Value LogicalAnd(CodeGen &codegen, const codegen::Value &o) const;
  codegen::Value LogicalOr(CodeGen &codegen, const codegen::Value &o) const;

  // Build a PHI node that combines all the given values (from their basic
  // blocks) into a single value
  static codegen::Value BuildPHI(
      CodeGen &codegen,
      const std::vector<std::pair<codegen::Value, llvm::BasicBlock *>> &vals);

  //===--------------------------------------------------------------------===//
  // Materialization helpers
  //===--------------------------------------------------------------------===//

  // Return the a representation of this value suitable for materialization
  void ValuesForMaterialization(CodeGen &codegen, llvm::Value *&val,
                                llvm::Value *&len, llvm::Value *&null) const;

  // Return a value that can be constructed from the provided type and value
  // registers
  static Value ValueFromMaterialization(type::Type::TypeId type,
                                        llvm::Value *val, llvm::Value *len,
                                        llvm::Value *null);

 private:
  friend class Hash;
  friend class CompactStorage;
  friend class UpdateableStorage;

  // Generate a hash for the given value
  void ValuesForHash(llvm::Value *&val, llvm::Value *&len) const;

 private:
  // The SQL type
  type::Type::TypeId type_;

  // The value
  llvm::Value *value_;

  // The length of the value (if it's variable in length)
  llvm::Value *length_;

  // NULL indicator (if any)
  llvm::Value *null_;

  // The (optional) name of this value
  std::string name_;
};

}  // namespace codegen
}  // namespace peloton
