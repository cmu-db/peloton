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

#include "include/codegen/utils/if.h"
#include "include/codegen/proxy/runtime_functions_proxy.h"
#include "codegen/type.h"

#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// CONSTRUCTORS
//===----------------------------------------------------------------------===//

Value::Value() : Value(type::Type::TypeId::INVALID) {}

Value::Value(type::Type::TypeId type, llvm::Value *val, llvm::Value *length,
             llvm::Value *null)
    : type_(type), value_(val), length_(length), null_(null) {}

//===----------------------------------------------------------------------===//
// COMPARISONS
//===----------------------------------------------------------------------===//

codegen::Value Value::CastTo(CodeGen &codegen,
                             type::Type::TypeId to_type) const {
  if (GetType() == to_type) {
    return *this;
  } else {
    const auto &cast = Type::GetCast(GetType(), to_type);
    return cast->DoCast(codegen, *this, to_type);
  }
}

// Equality comparison
Value Value::CompareEq(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareEq(codegen, *this, o);
}

// Inequality comparison
Value Value::CompareNe(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareNe(codegen, *this, o);
}

// Less-than comparison
Value Value::CompareLt(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareLt(codegen, *this, o);
}

// Less-then-or-equal comparison
Value Value::CompareLte(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareLte(codegen, *this, o);
}

// Greater-than comparison
Value Value::CompareGt(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareGt(codegen, *this, o);
}

// Greater-than-or-equal comparison
Value Value::CompareGte(CodeGen &codegen, const Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoCompareGte(codegen, *this, o);
}

// Sort for comparison
Value Value::CompareForSort(CodeGen &codegen, const codegen::Value &o) const {
  const auto &comparison = Type::GetComparison(o.GetType());
  return comparison->DoComparisonForSort(codegen, *this, o);
}

// Check that all the values from the left and equal to all the values in right
codegen::Value Value::TestEquality(CodeGen &codegen,
                                   const std::vector<codegen::Value> &lhs,
                                   const std::vector<codegen::Value> &rhs) {
  std::queue<codegen::Value, std::list<codegen::Value>> results;
  // Perform the comparison of each element of lhs to rhs
  for (size_t i = 0; i < lhs.size(); i++) {
    results.push(lhs[i].CompareEq(codegen, rhs[i]));
  }

  // Tournament-style collapse
  while (results.size() > 1) {
    codegen::Value first = results.front();
    results.pop();
    codegen::Value second = results.front();
    results.pop();
    results.push(first.LogicalAnd(codegen, second));
  }
  return results.front();
}

//===----------------------------------------------------------------------===//
// ARITHMETIC OPERATIONS
//===----------------------------------------------------------------------===//

// Addition
Value Value::Add(CodeGen &codegen, const Value &right, OnError on_error) const {
  auto *add_op = Type::GetBinaryOperator(Type::OperatorId::Add, GetType(),
                                         right.GetType());
  return add_op->DoWork(codegen, *this, right, on_error);
}

// Subtraction
Value Value::Sub(CodeGen &codegen, const Value &right, OnError on_error) const {
  auto *sub_op = Type::GetBinaryOperator(Type::OperatorId::Sub, GetType(),
                                         right.GetType());
  return sub_op->DoWork(codegen, *this, right, on_error);
}

// Multiplication
Value Value::Mul(CodeGen &codegen, const Value &right, OnError on_error) const {
  auto *mul_op = Type::GetBinaryOperator(Type::OperatorId::Mul, GetType(),
                                         right.GetType());
  return mul_op->DoWork(codegen, *this, right, on_error);
}

// Division
Value Value::Div(CodeGen &codegen, const Value &right, OnError on_error) const {
  auto *div_op = Type::GetBinaryOperator(Type::OperatorId::Div, GetType(),
                                         right.GetType());
  return div_op->DoWork(codegen, *this, right, on_error);
}

// Modulus
Value Value::Mod(CodeGen &codegen, const Value &right, OnError on_error) const {
  auto *mod_op = Type::GetBinaryOperator(Type::OperatorId::Mod, GetType(),
                                         right.GetType());
  return mod_op->DoWork(codegen, *this, right, on_error);
}

// Mathematical minimum
Value Value::Min(CodeGen &codegen, const Value &o) const {
  // Check if this < o
  auto is_lt = CompareLt(codegen, o);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_lt.GetValue(), GetValue(), o.GetValue());
  llvm::Value *len = nullptr;
  if (Type::HasVariableLength(GetType())) {
    len = codegen->CreateSelect(is_lt.GetValue(), GetLength(), o.GetLength());
  }
  return Value{GetType(), val, len};
}

// Mathematical maximum
Value Value::Max(CodeGen &codegen, const Value &o) const {
  // Check if this > o
  auto is_gt = CompareGt(codegen, o);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_gt.GetValue(), GetValue(), o.GetValue());
  llvm::Value *len = nullptr;
  if (Type::HasVariableLength(GetType())) {
    len = codegen->CreateSelect(is_gt.GetValue(), GetLength(), o.GetLength());
  }
  return Value{GetType(), val, len};
}

// Logical AND
Value Value::LogicalAnd(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == o.GetType());

  return Value{GetType(), codegen->CreateAnd(GetValue(), o.GetValue())};
}

// Logical OR
Value Value::LogicalOr(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == o.GetType());

  return Value{GetType(), codegen->CreateOr(GetValue(), o.GetValue())};
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
void Value::ValuesForMaterialization(llvm::Value *&val, llvm::Value *&len,
                                     llvm::Value *&null) const {
  PL_ASSERT(type_ != type::Type::TypeId::INVALID);
  val = GetValue();
  len = GetType() == type::Type::TypeId::VARCHAR ? GetLength() : nullptr;
  null = GetNull();
}

// Return the value that can be
Value Value::ValueFromMaterialization(type::Type::TypeId type, llvm::Value *val,
                                      llvm::Value *len, llvm::Value *null) {
  PL_ASSERT(type != type::Type::TypeId::INVALID);
  return Value{type, val, type == type::Type::TypeId::VARCHAR ? len : nullptr,
               null};
}

llvm::Value *Value::SetNullValue(CodeGen &codegen, const Value &value) {
  llvm::Value *null = nullptr;
  if (Type::HasVariableLength(value.GetType())) {
    null = codegen->CreateICmpEQ(value.GetValue(),
                                 codegen.NullPtr(codegen.CharPtrType()));
  } else {
    codegen::Value null_val = Type::GetNullValue(codegen, value.GetType());
    null = value.CompareEq(codegen, null_val).GetValue();
  }
  return null;
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
  if (Type::HasVariableLength(type)) {
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
