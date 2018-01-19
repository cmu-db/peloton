//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// decimal_type.cpp
//
// Identification: src/codegen/type/decimal_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/decimal_type.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/decimal_functions_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/value.h"
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
/// We do DECIMAL -> {INTEGRAL_TYPE, VARCHAR, BOOLEAN}
///
////////////////////////////////////////////////////////////////////////////////

struct CastDecimal : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const type::Type &from_type,
                     const type::Type &to_type) const override {
    if (from_type.GetSqlType() != Decimal::Instance()) {
      return false;
    }
    switch (to_type.type_id) {
      case peloton::type::TypeId::BOOLEAN:
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER:
      case peloton::type::TypeId::BIGINT:
        return true;
      default:
        return false;
    }
  }

  // Cast the given decimal value into the provided type
  Value Impl(CodeGen &codegen, const Value &value,
             const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));
    PL_ASSERT(!to_type.nullable);

    llvm::Type *val_type = nullptr, *len_type = nullptr;
    to_type.GetSqlType().GetTypeForMaterialization(codegen, val_type, len_type);

    llvm::Value *result = nullptr;
    switch (to_type.type_id) {
      case peloton::type::TypeId::BOOLEAN:
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER:
      case peloton::type::TypeId::BIGINT: {
        result = codegen->CreateFPToSI(value.GetValue(), val_type);
        break;
      }
      default: {
        throw NotImplementedException{
            StringUtil::Format("Cannot cast %s to %s",
                               TypeIdToString(value.GetType().type_id).c_str(),
                               TypeIdToString(to_type.type_id).c_str())};
      }
    }

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    return Value{to_type, result, nullptr, null};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

// Comparison
struct CompareDecimal : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Decimal::Instance() && left_type == right_type;
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpULT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpULE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUEQ(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUNE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUGT(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUGE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                           const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    llvm::Value *diff = codegen->CreateSub(left.GetValue(), right.GetValue());
    return Value{Integer::Instance(),
                 codegen->CreateFPToSI(diff, codegen.Int32Type()), nullptr,
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
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsType(val.GetType()));

    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        codegen.ConstDouble(0.0), val.GetValue(), overflow_bit);

    codegen.ThrowIfOverflow(overflow_bit);

    // Return result
    return Value{Decimal::Instance(), result, nullptr, nullptr};
  }
};

// Floor
struct Floor : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    llvm::Value *raw_ret =
        codegen.Call(DecimalFunctionsProxy::Floor, {val.GetValue()});
    return Value{Decimal::Instance(), raw_ret};
  }
};

// Round
struct Round : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Decimal::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    llvm::Value *raw_ret =
        codegen.Call(DecimalFunctionsProxy::Round, {val.GetValue()});
    return Value{Decimal::Instance(), raw_ret};
  }
};

// Ceiling
struct Ceil : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsType(val.GetType()));

    auto *result = codegen.Call(DecimalFunctionsProxy::Ceil, {val.GetValue()});

    return Value{Decimal::Instance(), result};
  }
};

// Sqrt
struct Sqrt : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Decimal::Instance();
  }

 protected:
  Value Impl(CodeGen &codegen, const Value &val,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    auto *raw_ret = codegen.Sqrt(val.GetValue());
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
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFAdd(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Subtraction
struct Sub : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFSub(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Multiplication
struct Mul : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFMul(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Division
struct Div : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    llvm::Value *div0 =
        codegen->CreateFCmpUEQ(right.GetValue(), codegen.ConstDouble(0.0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{Decimal::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = Decimal::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateFDiv(left.GetValue(), right.GetValue());
        division_result = Value{Decimal::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateFDiv(left.GetValue(), right.GetValue());
      result = Value{Decimal::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

// Modulo
struct Modulo : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    llvm::Value *div0 =
        codegen->CreateFCmpUEQ(right.GetValue(), codegen.ConstDouble(0.0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{Decimal::Instance()};

    if (ctx.on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0, "div0"};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = Decimal::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateFRem(left.GetValue(), right.GetValue());
        division_result = Value{Decimal::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (ctx.on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateFRem(left.GetValue(), right.GetValue());
      result = Value{Decimal::Instance(), raw_val, nullptr, nullptr};
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
    peloton::type::TypeId::DECIMAL};

// Explicit casting rules
CastDecimal kCastDecimal;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::BOOLEAN,
     kCastDecimal},
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::TINYINT,
     kCastDecimal},
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::SMALLINT,
     kCastDecimal},
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::INTEGER,
     kCastDecimal},
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::BIGINT,
     kCastDecimal},
    {peloton::type::TypeId::DECIMAL, peloton::type::TypeId::DECIMAL,
     kCastDecimal}};

// Comparison operations
CompareDecimal kCompareDecimal;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareDecimal}};

// Unary operators
Negate kNegOp;
Floor kFloorOp;
Round kRound;
Ceil kCeilOp;
Sqrt kSqrt;
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {
    {OperatorId::Negation, kNegOp},
    {OperatorId::Floor, kFloorOp},
    {OperatorId::Round, kRound},
    {OperatorId::Ceil, kCeilOp},
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
/// DECIMAL type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Decimal::Decimal()
    : SqlType(peloton::type::TypeId::DECIMAL),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

Value Decimal::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Decimal::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Decimal::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstDouble(peloton::type::PELOTON_DECIMAL_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void Decimal::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                        llvm::Type *&len_type) const {
  val_type = codegen.DoubleType();
  len_type = nullptr;
}

llvm::Function *Decimal::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  // TODO: We should be using the precision/scale in the output function
  return ValuesRuntimeProxy::OutputDecimal.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
