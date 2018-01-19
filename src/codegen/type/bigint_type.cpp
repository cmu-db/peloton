//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bigint_type.cpp
//
// Identification: src/codegen/type/bigint_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/bigint_type.h"

#include "codegen/lang/if.h"
#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "common/exception.h"
#include "type/limits.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

////////////////////////////////////////////////////////////////////////////////
///
/// Casting
///
/// We do BIGINT -> {INTEGRAL_TYPE, DECIMAL, VARCHAR, BOOLEAN}
///
////////////////////////////////////////////////////////////////////////////////

struct CastBigInt : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    if (from_type.GetSqlType() != BigInt::Instance()) {
      return false;
    }
    switch (to_type.GetSqlType().TypeId()) {
      case peloton::type::TypeId::BOOLEAN:
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER:
      case peloton::type::TypeId::BIGINT:
      case peloton::type::TypeId::DECIMAL:
        return true;
      default:
        return false;
    }
  }

  Value Impl(CodeGen &codegen, const Value &value,
             const Type &to_type) const override {
    llvm::Value *result = nullptr;
    switch (to_type.GetSqlType().TypeId()) {
      case peloton::type::TypeId::BOOLEAN: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.BoolType());
        break;
      }
      case peloton::type::TypeId::TINYINT: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.Int8Type());
        break;
      }
      case peloton::type::TypeId::SMALLINT: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.Int16Type());
        break;
      }
      case peloton::type::TypeId::INTEGER: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.Int32Type());
        break;
      }
      case peloton::type::TypeId::BIGINT: {
        result = value.GetValue();
        break;
      }
      case peloton::type::TypeId::DECIMAL: {
        result = codegen->CreateSIToFP(value.GetValue(), codegen.DoubleType());
        break;
      }
      default: {
        throw Exception{
            StringUtil::Format("Cannot cast %s to %s",
                               TypeIdToString(value.GetType().type_id).c_str(),
                               TypeIdToString(to_type.type_id).c_str())};
      }
    }

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    // Return the result
    return Value{to_type, result, nullptr, null};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

// Comparison
struct CompareBigInt : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == BigInt::Instance() && left_type == right_type;
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateICmpEQ(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateICmpNE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                           const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    llvm::Value *diff = codegen->CreateSub(left.GetValue(), right.GetValue());
    return Value{Integer::Instance(),
                 codegen->CreateSExt(diff, codegen.Int32Type()), nullptr,
                 nullptr};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Unary operations
///
////////////////////////////////////////////////////////////////////////////////

// Negation
struct Negate : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == BigInt::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsType(val.GetType()));

    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        codegen.Const64(0), val.GetValue(), overflow_bit);

    codegen.ThrowIfOverflow(overflow_bit);

    // Return result
    return Value{BigInt::Instance(), result, nullptr, nullptr};
  }
};

// Floor
struct Floor : public TypeSystem::UnaryOperatorHandleNull {
  CastBigInt cast;

  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == BigInt::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsType(val.GetType()));
    return cast.Impl(codegen, val, Decimal::Instance());
  }
};

// Ceiling
struct Ceil : public TypeSystem::UnaryOperatorHandleNull {
  CastBigInt cast;

  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == BigInt::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsType(val.GetType()));
    return cast.Impl(codegen, val, Decimal::Instance());
  }
};

// Sqrt
struct Sqrt : public TypeSystem::UnaryOperatorHandleNull {
  CastBigInt cast;

  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == BigInt::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Decimal::Instance();
  }

 protected:
  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
  const override {
    auto casted = cast.Impl(codegen, val, Decimal::Instance());
    auto *raw_ret = codegen.Sqrt(casted.GetValue());
    return Value{Decimal::Instance(), raw_ret};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Binary operations
///
////////////////////////////////////////////////////////////////////////////////

// Addition
struct Add : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == BigInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallAddWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (ctx.on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{BigInt::Instance(), result, nullptr, nullptr};
  }
};

// Subtraction
struct Sub : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == BigInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do subtraction
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (ctx.on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{BigInt::Instance(), result};
  }
};

// Multiplication
struct Mul : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == BigInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do multiplication
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallMulWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (ctx.on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{BigInt::Instance(), result};
  }
};

// Division
struct Div : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == BigInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const64(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{BigInt::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = BigInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
        division_result = Value{BigInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
      result = Value{BigInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

// Modulo
struct Modulo : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == BigInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{BigInt::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const64(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{BigInt::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = BigInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
        division_result = Value{BigInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
      result = Value{BigInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::BIGINT, peloton::type::TypeId::DECIMAL};

// Explicit casts
CastBigInt kCastBigInt;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::BOOLEAN,
     kCastBigInt},
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::TINYINT,
     kCastBigInt},
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::SMALLINT,
     kCastBigInt},
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::INTEGER,
     kCastBigInt},
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::BIGINT, kCastBigInt},
    {peloton::type::TypeId::BIGINT, peloton::type::TypeId::DECIMAL,
     kCastBigInt}};

// Comparison operations
CompareBigInt kCompareBigInt;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareBigInt}};

// Unary operators
Negate kNegOp;
Ceil kCeilOp;
Floor kFloorOp;
Sqrt kSqrt;
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {
    {OperatorId::Negation, kNegOp},
    {OperatorId::Ceil, kCeilOp},
    {OperatorId::Floor, kFloorOp},
    {OperatorId::Sqrt, kSqrt}};

// Binary operations
Add kAddOp;
Sub kSubOp;
Mul kMulOp;
Div kDivOp;
Modulo kModuloOp;
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::Add, kAddOp},
    {OperatorId::Sub, kSubOp},
    {OperatorId::Mul, kMulOp},
    {OperatorId::Div, kDivOp},
    {OperatorId::Mod, kModuloOp}};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// No arg operations
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// BIGINT type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

BigInt::BigInt()
    : SqlType(peloton::type::TypeId::BIGINT),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

Value BigInt::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_INT64_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value BigInt::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_INT64_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value BigInt::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_INT64_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void BigInt::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                       llvm::Type *&len_type) const {
  val_type = codegen.Int64Type();
  len_type = nullptr;
}

llvm::Function *BigInt::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::OutputBigInt.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
