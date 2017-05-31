//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.cpp
//
// Identification: src/codegen/value.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value.h"

#include <list>
#include <queue>

#include "codegen/if.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/type.h"

#include "type/value.h"

namespace peloton {
namespace codegen {

Value::Value() : Value(type::Type::TypeId::INVALID) {}

Value::Value(type::Type::TypeId type, llvm::Value *val, llvm::Value *length,
             llvm::Value *null)
    : type_(type), value_(val), length_(length), null_(null) {}

//===----------------------------------------------------------------------===//
// Utility functions
//===----------------------------------------------------------------------===//

llvm::Value *Value::ReifyBoolean(CodeGen &codegen) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  llvm::Value *null_bit = GetNullBit();
  if (null_bit == nullptr) {
    return GetValue();
  } else {
    // The value is nullable, need to check the null bit
    return codegen->CreateSelect(null_bit, codegen.ConstBool(false),
                                 GetValue());
  }
}

// Return a boolean value indicating whether this value is NULL
llvm::Value *Value::IsNull(CodeGen &codegen) const {
  return (GetNullBit() != nullptr ? GetNullBit() : codegen.ConstBool(false));
}

// Return a boolean (i1) value indicating whether this value is not NULL
llvm::Value *Value::IsNotNull(CodeGen &codegen) const {
  return codegen->CreateNot(IsNull(codegen));
}

//===----------------------------------------------------------------------===//
// COMPARISONS
//===----------------------------------------------------------------------===//

Value Value::CastTo(CodeGen &codegen, type::Type::TypeId to_type) const {
  // If the type we're casting to is the type of the value, we're done
  if (GetType() == to_type) {
    return *this;
  }

  // Do the explicit cast
  const auto *cast = Type::GetCast(GetType(), to_type);
  return cast->DoCast(codegen, *this, to_type);
}

Value Value::CompareEq(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareEq(codegen, left, right);
}

Value Value::CompareNe(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareNe(codegen, left, right);
}

Value Value::CompareLt(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareLt(codegen, left, right);
}

Value Value::CompareLte(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareLte(codegen, left, right);
}

Value Value::CompareGt(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareGt(codegen, left, right);
}

Value Value::CompareGte(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoCompareGte(codegen, left, right);
}

Value Value::CompareForSort(CodeGen &codegen, const Value &other) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  const auto *comparison =
      Type::GetComparison(GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return comparison->DoComparisonForSort(codegen, left, right);
}

// Check that all the values from the left and equal to all the values in right
Value Value::TestEquality(CodeGen &codegen, const std::vector<Value> &lhs,
                          const std::vector<Value> &rhs) {
  std::queue<Value, std::list<Value>> results;
  // Perform the comparison of each element of lhs to rhs
  for (size_t i = 0; i < lhs.size(); i++) {
    results.push(lhs[i].CompareEq(codegen, rhs[i]));
  }

  // Tournament-style collapse
  while (results.size() > 1) {
    Value first = results.front();
    results.pop();
    Value second = results.front();
    results.pop();
    results.push(first.LogicalAnd(codegen, second));
  }
  return results.front();
}

//===----------------------------------------------------------------------===//
// ARITHMETIC OPERATIONS
//===----------------------------------------------------------------------===//

// Addition
Value Value::Add(CodeGen &codegen, const Value &other, OnError on_error) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  auto *add_op = Type::GetBinaryOperator(
      Type::OperatorId::Add, GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return add_op->DoWork(codegen, left, right, on_error);
}

// Subtraction
Value Value::Sub(CodeGen &codegen, const Value &other, OnError on_error) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  auto *sub_op = Type::GetBinaryOperator(
      Type::OperatorId::Sub, GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return sub_op->DoWork(codegen, left, right, on_error);
}

// Multiplication
Value Value::Mul(CodeGen &codegen, const Value &other, OnError on_error) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  auto *mul_op = Type::GetBinaryOperator(
      Type::OperatorId::Mul, GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return mul_op->DoWork(codegen, left, right, on_error);
}

// Division
Value Value::Div(CodeGen &codegen, const Value &other, OnError on_error) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  auto *div_op = Type::GetBinaryOperator(
      Type::OperatorId::Div, GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return div_op->DoWork(codegen, left, right, on_error);
}

// Modulus
Value Value::Mod(CodeGen &codegen, const Value &other, OnError on_error) const {
  type::Type::TypeId left_cast = GetType();
  type::Type::TypeId right_cast = other.GetType();

  auto *mod_op = Type::GetBinaryOperator(
      Type::OperatorId::Mod, GetType(), left_cast, other.GetType(), right_cast);

  Value left = CastTo(codegen, left_cast);
  Value right = other.CastTo(codegen, right_cast);

  return mod_op->DoWork(codegen, left, right, on_error);
}

// Mathematical minimum
Value Value::Min(CodeGen &codegen, const Value &other) const {
  // Check if this < o
  auto is_lt = CompareLt(codegen, other);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_lt.GetValue(), GetValue(), other.GetValue());
  llvm::Value *len = nullptr;
  if (Type::IsVariableLength(GetType())) {
    len =
        codegen->CreateSelect(is_lt.GetValue(), GetLength(), other.GetLength());
  }
  return Value{GetType(), val, len};
}

// Mathematical maximum
Value Value::Max(CodeGen &codegen, const Value &other) const {
  // Check if this > other
  auto is_gt = CompareGt(codegen, other);

  // Choose either this or other depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_gt.GetValue(), GetValue(), other.GetValue());
  llvm::Value *len = nullptr;
  if (Type::IsVariableLength(GetType())) {
    len =
        codegen->CreateSelect(is_gt.GetValue(), GetLength(), other.GetLength());
  }
  return Value{GetType(), val, len};
}

// TODO: AND and OR need to handle NULL

// Logical AND
Value Value::LogicalAnd(CodeGen &codegen, const Value &other) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == other.GetType());

  return Value{GetType(), codegen->CreateAnd(GetValue(), other.GetValue())};
}

// Logical OR
Value Value::LogicalOr(CodeGen &codegen, const Value &other) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == other.GetType());

  return Value{GetType(), codegen->CreateOr(GetValue(), other.GetValue())};
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForHash(llvm::Value *&val, llvm::Value *&len) const {
  val = GetValue();
  if (GetType() == type::Type::TypeId::VARCHAR ||
      GetType() == type::Type::TypeId::VARBINARY) {
    len = GetLength();
  } else {
    len = nullptr;
  }
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForMaterialization(CodeGen &codegen, llvm::Value *&val,
                                     llvm::Value *&len,
                                     llvm::Value *&null) const {
  PL_ASSERT(type_ != type::Type::TypeId::INVALID);
  val = GetValue();
  len = GetType() == type::Type::TypeId::VARCHAR ? GetLength() : nullptr;
  null = IsNull(codegen);
}

// Return the value that can be
Value Value::ValueFromMaterialization(type::Type::TypeId type, llvm::Value *val,
                                      llvm::Value *len, llvm::Value *null) {
  PL_ASSERT(type != type::Type::TypeId::INVALID);
  return Value{type, val, (type == type::Type::TypeId::VARCHAR ? len : nullptr),
               null};
}

// Build a new value that combines values arriving from different BB's into a
// single value.
Value Value::BuildPHI(
    CodeGen &codegen,
    const std::vector<std::pair<Value, llvm::BasicBlock *>> &vals) {
  PL_ASSERT(vals.size() > 0);
  uint32_t num_entries = static_cast<uint32_t>(vals.size());

  // The SQL type of the values that we merge here
  // TODO: Need to make sure all incoming types are unifyable
  auto type = vals[0].first.GetType();

  // Get the LLVM type for the values
  llvm::Type *val_type = nullptr, *len_type = nullptr;
  Type::GetTypeForMaterialization(codegen, type, val_type, len_type);
  PL_ASSERT(val_type != nullptr);

  // Do the merge depending on the type
  if (Type::IsVariableLength(type)) {
    PL_ASSERT(len_type != nullptr);
    auto *val_phi = codegen->CreatePHI(val_type, num_entries);
    auto *len_phi = codegen->CreatePHI(len_type, num_entries);
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      len_phi->addIncoming(val_pair.first.GetLength(), val_pair.second);
    }
    return Value{type, val_phi, len_phi};
  } else {
    PL_ASSERT(len_type == nullptr);
    auto *phi = codegen->CreatePHI(val_type, num_entries);
    for (const auto &val_pair : vals) {
      phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
    }
    return Value{type, phi, nullptr};
  }
}

}  // namespace codegen
}  // namespace peloton
