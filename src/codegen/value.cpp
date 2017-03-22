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

#include "codegen/if.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/type.h"

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

//===----------------------------------------------------------------------===//
// ARITHMETIC OPERATIONS
//===----------------------------------------------------------------------===//

// Addition
Value Value::Add(CodeGen &codegen, const Value &o) const {
  auto *add_op = Type::GetBinaryOperator(Type::BinaryOperatorId::Add, GetType(),
                                         o.GetType());
  return add_op->DoWork(codegen, *this, o);
}

// Subtraction
Value Value::Sub(CodeGen &codegen, const Value &o) const {
  auto *sub_op = Type::GetBinaryOperator(Type::BinaryOperatorId::Sub, GetType(),
                                         o.GetType());
  return sub_op->DoWork(codegen, *this, o);
}

// Multiplication
Value Value::Mul(CodeGen &codegen, const Value &o) const {
  auto *mul_op = Type::GetBinaryOperator(Type::BinaryOperatorId::Mul, GetType(),
                                         o.GetType());
  return mul_op->DoWork(codegen, *this, o);
}

// Division
Value Value::Div(CodeGen &codegen, const Value &o) const {
  auto *div_op = Type::GetBinaryOperator(Type::BinaryOperatorId::Div, GetType(),
                                         o.GetType());
  return div_op->DoWork(codegen, *this, o);
}

// Modulus
Value Value::Mod(CodeGen &codegen, const Value &o) const {
  auto *mod_op = Type::GetBinaryOperator(Type::BinaryOperatorId::Mod, GetType(),
                                         o.GetType());
  return mod_op->DoWork(codegen, *this, o);
}

// Mathematical minimum
Value Value::Min(CodeGen &codegen, const Value &o) const {
  // Check if this < o
  auto *comparison = Type::GetComparison(GetType());
  auto is_lt = comparison->DoCompareLt(codegen, *this, o);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_lt.GetValue(), GetValue(), o.GetValue());
  llvm::Value *len = nullptr;
  if (Type::HasVariableLength(GetType())) {
    len = codegen->CreateSelect(is_lt.GetValue(), GetValue(), o.GetValue());
  }
  return Value{GetType(), val, len};
}

// Mathematical maximum
Value Value::Max(CodeGen &codegen, const Value &o) const {
  // Check if this > o
  auto *comparison = Type::GetComparison(GetType());
  auto is_gt = comparison->DoCompareGt(codegen, *this, o);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_gt.GetValue(), GetValue(), o.GetValue());
  llvm::Value *len = nullptr;
  if (Type::HasVariableLength(GetType())) {
    len = codegen->CreateSelect(is_gt.GetValue(), GetValue(), o.GetValue());
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
void Value::ValuesForHash(UNUSED_ATTRIBUTE CodeGen &codegen, llvm::Value *&val,
                          llvm::Value *&len) const {
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
void Value::ValuesForMaterialization(UNUSED_ATTRIBUTE CodeGen &codegen,
                                     llvm::Value *&val,
                                     llvm::Value *&len) const {
  PL_ASSERT(type_ != type::Type::TypeId::INVALID);
  val = GetValue();
  len = GetType() == type::Type::TypeId::VARCHAR ? GetLength() : nullptr;
}

//===----------------------------------------------------------------------===//
// Return the types of this value suitable for materialization.  The types
// should be such that
//===----------------------------------------------------------------------===//
void Value::TypeForMaterialization(CodeGen &codegen,
                                   type::Type::TypeId value_type,
                                   llvm::Type *&val_type,
                                   llvm::Type *&len_type) {
  PL_ASSERT(value_type != type::Type::TypeId::INVALID);
  switch (value_type) {
    case type::Type::TypeId::BOOLEAN:
      val_type = codegen.BoolType();
      break;
    case type::Type::TypeId::TINYINT:
      val_type = codegen.Int8Type();
      break;
    case type::Type::TypeId::SMALLINT:
      val_type = codegen.Int16Type();
      break;
    case type::Type::TypeId::INTEGER:
      val_type = codegen.Int32Type();
      break;
    case type::Type::TypeId::BIGINT:
      val_type = codegen.Int64Type();
      break;
    case type::Type::TypeId::DECIMAL:
      val_type = codegen.DoubleType();
      break;
    case type::Type::TypeId::VARCHAR:
      val_type = codegen.CharPtrType();
      len_type = codegen.Int32Type();
      break;
    default: {
      throw Exception{"Can't materialized type: " + TypeIdToString(value_type)};
    }
  }
}

// Return the value that can be
Value Value::ValueFromMaterialization(UNUSED_ATTRIBUTE CodeGen &codegen,
                                      type::Type::TypeId type, llvm::Value *val,
                                      llvm::Value *len) {
  PL_ASSERT(type != type::Type::TypeId::INVALID);
  return Value{type, val, type == type::Type::TypeId::VARCHAR ? len : nullptr};
}

// Get the LLVM type that matches the numeric type provided
llvm::Type *Value::NumericType(CodeGen &codegen, type::Type::TypeId type) {
  switch (type) {
    case type::Type::TypeId::BOOLEAN:
      return codegen.BoolType();
    case type::Type::TypeId::TINYINT:
      return codegen.Int8Type();
    case type::Type::TypeId::SMALLINT:
      return codegen.Int16Type();
    case type::Type::TypeId::DATE:
    case type::Type::TypeId::INTEGER:
      return codegen.Int32Type();
    case type::Type::TypeId::TIMESTAMP:
    case type::Type::TypeId::BIGINT:
      return codegen.Int64Type();
    case type::Type::TypeId::DECIMAL:
      return codegen.DoubleType();
    default:
      throw Exception{TypeIdToString(type) + " is not a numeric type"};
  }
}

// Build a new value that combines values arriving from different BB's into a
// single value.
Value Value::BuildPHI(
    CodeGen &codegen,
    const std::vector<std::pair<Value, llvm::BasicBlock *>> &vals) {
  PL_ASSERT(vals.size() > 0);
  // TODO: Need to make sure all incoming types are unifyable
  auto type = vals[0].first.GetType();
  if (type == type::Type::TypeId::VARCHAR ||
      type == type::Type::TypeId::VARBINARY) {
    auto *val_phi = codegen->CreatePHI(codegen.CharPtrType(), vals.size());
    auto *len_phi = codegen->CreatePHI(codegen.Int32Type(), vals.size());
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      len_phi->addIncoming(val_pair.first.GetLength(), val_pair.second);
    }
    return Value{type, val_phi, len_phi};
  } else {
    auto *phi = codegen->CreatePHI(NumericType(codegen, type), vals.size());
    for (const auto &val_pair : vals) {
      phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
    }
    return Value{type, phi, nullptr};
  }
}

//===----------------------------------------------------------------------===//
// Create a conditional branch to throw a divide by zero exception
//===----------------------------------------------------------------------===//
void Value::CreateCheckDivideByZeroException(CodeGen &codegen,
                                             const codegen::Value &divisor) {
  // Check if divisor is zero
  llvm::Constant *zero =
      llvm::ConstantInt::get(NumericType(codegen, divisor.GetType()), 0, true);
  If val_is_zero{codegen, codegen->CreateICmpEQ(divisor.GetValue(), zero)};
  {
    // Throw DivideByZeroException
    codegen.CallFunc(
        RuntimeFunctionsProxy::_ThrowDivideByZeroException::GetFunction(
            codegen),
        {});
  }
  val_is_zero.EndIf();
}

//===----------------------------------------------------------------------===//
// Create a conditional branch to throw an overflow exception
//===----------------------------------------------------------------------===//
void Value::CreateCheckOverflowException(CodeGen &codegen,
                                         llvm::Value *overflow_bit) {
  // Generate if statement to check for overflow
  If has_overflow{codegen,
                  codegen->CreateICmpEQ(overflow_bit, codegen.ConstBool(1))};
  {
    // Throw overflow exception if the overflow bit is set
    codegen.CallFunc(
        RuntimeFunctionsProxy::_ThrowOverflowException::GetFunction(codegen),
        {});
  }
  has_overflow.EndIf();
}

}  // namespace codegen
}  // namespace peloton