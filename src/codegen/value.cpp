//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.cpp
//
// Identification: src/codegen/value.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value.h"

#include <list>
#include <queue>

#include "codegen/type/type_system.h"
#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {

Value::Value() : Value(type::Type{peloton::type::TypeId::INVALID, false}) {}

Value::Value(const type::Type &type, llvm::Value *val, llvm::Value *length,
             llvm::Value *null)
    : type_(type), value_(val), length_(length), null_(null) {
  // If the value is NULL-able, it better have an accompanying NULL bit
  PL_ASSERT(!type_.nullable || null_ != nullptr);
}

// Return a boolean value indicating whether this value is NULL
llvm::Value *Value::IsNull(CodeGen &codegen) const {
  if (IsNullable()) {
    PL_ASSERT(null_ != nullptr && null_->getType() == codegen.BoolType());
    return null_;
  } else {
    return codegen.ConstBool(false);
  }
}

// Return a boolean (i1) value indicating whether this value is not NULL
llvm::Value *Value::IsNotNull(CodeGen &codegen) const {
  return codegen->CreateNot(IsNull(codegen));
}

//===----------------------------------------------------------------------===//
// COMPARISONS
//===----------------------------------------------------------------------===//

Value Value::CastTo(CodeGen &codegen, const type::Type &to_type) const {
  // If the type we're casting to is the type of the value, we're done
  if (GetType() == to_type) {
    return *this;
  }

  // Lookup the cast operation and execute it
  const auto *cast_op = type::TypeSystem::GetCast(GetType(), to_type);
  PL_ASSERT(cast_op != nullptr);
  return cast_op->Eval(codegen, *this, to_type);
}

#define GEN_COMPARE(OP)                                     \
  type::Type left_cast = GetType();                         \
  type::Type right_cast = other.GetType();                  \
                                                            \
  const auto *comparison = type::TypeSystem::GetComparison( \
      GetType(), left_cast, other.GetType(), right_cast);   \
                                                            \
  PL_ASSERT(comparison != nullptr);                         \
  Value left = CastTo(codegen, left_cast);                  \
  Value right = other.CastTo(codegen, right_cast);          \
                                                            \
  return comparison->Eval##OP(codegen, left, right);

Value Value::CompareEq(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareEq);
}

Value Value::CompareNe(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareNe);
}

Value Value::CompareLt(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareLt);
}

Value Value::CompareLte(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareLte);
}

Value Value::CompareGt(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareGt);
}

Value Value::CompareGte(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareGte);
}

Value Value::CompareForSort(CodeGen &codegen, const Value &other) const {
  GEN_COMPARE(CompareForSort);
}

#undef GEN_COMPARE

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
  return CallBinaryOp(codegen, OperatorId::Add, other, on_error);
}

// Subtraction
Value Value::Sub(CodeGen &codegen, const Value &other, OnError on_error) const {
  return CallBinaryOp(codegen, OperatorId::Sub, other, on_error);
}

// Multiplication
Value Value::Mul(CodeGen &codegen, const Value &other, OnError on_error) const {
  return CallBinaryOp(codegen, OperatorId::Mul, other, on_error);
}

// Division
Value Value::Div(CodeGen &codegen, const Value &other, OnError on_error) const {
  return CallBinaryOp(codegen, OperatorId::Div, other, on_error);
}

// Modulus
Value Value::Mod(CodeGen &codegen, const Value &other, OnError on_error) const {
  return CallBinaryOp(codegen, OperatorId::Mod, other, on_error);
}

// Logical AND
Value Value::LogicalAnd(CodeGen &codegen, const Value &other) const {
  return CallBinaryOp(codegen, OperatorId::LogicalAnd, other,
                      OnError::Exception);
}

// Logical OR
Value Value::LogicalOr(CodeGen &codegen, const Value &other) const {
  return CallBinaryOp(codegen, OperatorId::LogicalOr, other,
                      OnError::Exception);
}

// TODO: Min/Max need to handle NULL

// Mathematical minimum
Value Value::Min(CodeGen &codegen, const Value &other) const {
  // Check if this < o
  auto is_lt = CompareLt(codegen, other);

  // Choose either this or o depending on result of comparison
  llvm::Value *val =
      codegen->CreateSelect(is_lt.GetValue(), GetValue(), other.GetValue());
  llvm::Value *len = nullptr;
  if (GetType().GetSqlType().IsVariableLength()) {
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
  if (GetType().GetSqlType().IsVariableLength()) {
    len =
        codegen->CreateSelect(is_gt.GetValue(), GetLength(), other.GetLength());
  }
  return Value{GetType(), val, len};
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForHash(llvm::Value *&val, llvm::Value *&len) const {
  PL_ASSERT(GetType().type_id != peloton::type::TypeId::INVALID);
  val = GetValue();
  len = GetType().GetSqlType().IsVariableLength() ? GetLength() : nullptr;
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForMaterialization(CodeGen &codegen, llvm::Value *&val,
                                     llvm::Value *&len,
                                     llvm::Value *&null) const {
  PL_ASSERT(GetType().type_id != peloton::type::TypeId::INVALID);
  val = GetValue();
  len = GetType().GetSqlType().IsVariableLength() ? GetLength() : nullptr;
  null = IsNull(codegen);
}

// Return the value that can be
Value Value::ValueFromMaterialization(const type::Type &type, llvm::Value *val,
                                      llvm::Value *len, llvm::Value *null) {
  PL_ASSERT(type.type_id != peloton::type::TypeId::INVALID);
  return Value{type, val,
               (type.GetSqlType().IsVariableLength() ? len : nullptr),
               (type.nullable ? null : nullptr)};
}

// Build a new value that combines values arriving from different BB's into a
// single value.
Value Value::BuildPHI(
    CodeGen &codegen,
    const std::vector<std::pair<Value, llvm::BasicBlock *>> &vals) {
  const auto num_entries = static_cast<uint32_t>(vals.size());

  // The SQL type of the values that we merge here
  // TODO: Need to make sure all incoming types are unifyable
  const type::Type &type = vals[0].first.GetType();
  const type::SqlType &sql_type = type.GetSqlType();

  // Get the LLVM type for the values
  llvm::Type *null_type = codegen.BoolType();
  llvm::Type *val_type = nullptr, *len_type = nullptr;
  sql_type.GetTypeForMaterialization(codegen, val_type, len_type);
  PL_ASSERT(val_type != nullptr);

  // Do the merge depending on the type
  if (sql_type.IsVariableLength()) {
    PL_ASSERT(len_type != nullptr);
    auto *val_phi = codegen->CreatePHI(val_type, num_entries);
    auto *len_phi = codegen->CreatePHI(len_type, num_entries);
    auto *null_phi = codegen->CreatePHI(null_type, num_entries);
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      len_phi->addIncoming(val_pair.first.GetLength(), val_pair.second);
      null_phi->addIncoming(val_pair.first.IsNull(codegen), val_pair.second);
    }
    return Value{type, val_phi, len_phi, null_phi};
  } else {
    PL_ASSERT(len_type == nullptr);
    auto *val_phi = codegen->CreatePHI(val_type, num_entries);
    auto *null_phi = codegen->CreatePHI(null_type, num_entries);
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      null_phi->addIncoming(val_pair.first.IsNull(codegen), val_pair.second);
    }
    return Value{type, val_phi, nullptr, null_phi};
  }
}

Value Value::CallUnaryOp(CodeGen &codegen, OperatorId op_id) const {
  // Lookup the operation in the value's type system
  auto *unary_op = type::TypeSystem::GetUnaryOperator(op_id, GetType());
  PL_ASSERT(unary_op != nullptr);

  // Setup the invocation context
  type::TypeSystem::InvocationContext ctx{.on_error = OnError::Exception,
                                          .executor_context = nullptr};

  // Invoke
  return unary_op->Eval(codegen, *this, ctx);
}

Value Value::CallBinaryOp(CodeGen &codegen, OperatorId op_id,
                          const Value &other, OnError on_error) const {
  type::Type left_target_type = GetType();
  type::Type right_target_type = other.GetType();

  // Lookup the operation in the type system
  auto *binary_op = type::TypeSystem::GetBinaryOperator(
      op_id, GetType(), left_target_type, other.GetType(), right_target_type);
  PL_ASSERT(binary_op != nullptr);

  // Cast input types as need be
  Value casted_left = CastTo(codegen, left_target_type);
  Value casted_right = other.CastTo(codegen, right_target_type);

  // Setup the invocation context
  type::TypeSystem::InvocationContext ctx{.on_error = on_error,
                                          .executor_context = nullptr};

  // Invoke
  return binary_op->Eval(codegen, casted_left, casted_right, ctx);
}

}  // namespace codegen
}  // namespace peloton
