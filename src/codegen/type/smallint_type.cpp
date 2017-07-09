//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// smallint_type.cpp
//
// Identification: src/codegen/type/smallint_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/smallint_type.h"

#include "codegen/lang/if.h"
#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "common/exception.h"
#include "type/limits.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

//===----------------------------------------------------------------------===//
// CASTING RULES
//
// We do SMALLINT -> {INTEGRAL_TYPE, DECIMAL, VARCHAR, BOOLEAN}
//===----------------------------------------------------------------------===//

struct CastSmallInt : public TypeSystem::Cast {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    if (from_type.GetSqlType() != SmallInt::Instance()) {
      return false;
    }
    switch (to_type.GetSqlType().TypeId()) {
      case peloton::type::TypeId::BOOLEAN:
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER:
      case peloton::type::TypeId::BIGINT:
      case peloton::type::TypeId::DECIMAL:
      case peloton::type::TypeId::VARCHAR:
        return true;
      default:
        return false;
    }
  }

  Value DoCast(CodeGen &codegen, const Value &value,
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
        result = value.GetValue();
        break;
      }
      case peloton::type::TypeId::INTEGER: {
        result = codegen->CreateSExt(value.GetValue(), codegen.Int32Type());
        break;
      }
      case peloton::type::TypeId::BIGINT: {
        result = codegen->CreateSExt(value.GetValue(), codegen.Int64Type());
        break;
      }
      case peloton::type::TypeId::DECIMAL: {
        result = codegen->CreateSIToFP(value.GetValue(), codegen.DoubleType());
        break;
      }
      case peloton::type::TypeId::VARCHAR: {
        result = codegen->CreateTrunc(value.GetValue(), codegen.BoolType());
        break;
      }
      default: {
        throw Exception{
            StringUtil::Format("Cannot cast %s to %s",
                               TypeIdToString(value.GetType().type_id).c_str(),
                               TypeIdToString(to_type.type_id).c_str())};
      }
    }

    // Return the result
    return Value{to_type, result, nullptr, nullptr};
  }
};

//===----------------------------------------------------------------------===//
// COMPARISON
//===----------------------------------------------------------------------===//

// Comparison
struct CompareSmallInt : public TypeSystem::Comparison {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == SmallInt::Instance() && left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSLE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateICmpEQ(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateICmpNE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    auto *raw_val = codegen->CreateICmpSGE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    llvm::Value *diff = codegen->CreateSub(left.GetValue(), right.GetValue());
    return Value{Integer::Instance(),
                 codegen->CreateSExt(diff, codegen.Int32Type()), nullptr,
                 nullptr};
  }
};

//===----------------------------------------------------------------------===//
// UNARY OPERATIONS
//===----------------------------------------------------------------------===//

// Negation
struct Negate : public TypeSystem::UnaryOperator {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == SmallInt::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &val) const override {
    PL_ASSERT(SupportsType(val.GetType()));

    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        codegen.Const16(0), val.GetValue(), overflow_bit);

    codegen.ThrowIfOverflow(overflow_bit);

    // Return result
    return Value{SmallInt::Instance(), result, nullptr, nullptr};
  }
};

//===----------------------------------------------------------------------===//
// BINARY OPERATIONS
//===----------------------------------------------------------------------===//

// Addition
struct Add : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallAddWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{SmallInt::Instance(), result, nullptr, nullptr};
  }
};

// Subtraction
struct Sub : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do subtraction
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{SmallInt::Instance(), result};
  }
};

// Multiplication
struct Mul : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do multiplication
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallMulWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{SmallInt::Instance(), result};
  }
};

// Division
struct Div : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const16(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{SmallInt::Instance()};

    if (on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = SmallInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
        division_result =
            Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSDiv(left.GetValue(), right.GetValue());
      result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

// Modulo
struct Modulo : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == SmallInt::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{SmallInt::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    auto *div0 = codegen->CreateICmpEQ(right.GetValue(), codegen.Const16(0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{SmallInt::Instance()};

    if (on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0};
      {
        // The divisor is 0, return NULL because that's what the caller wants
        default_val = SmallInt::Instance().GetNullValue(codegen);
      }
      is_div0.ElseBlock();
      {
        // The divisor isn't 0, do the division
        auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
        division_result =
            Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);

    } else if (on_error == OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0);

      // Do division
      auto *raw_val = codegen->CreateSRem(left.GetValue(), right.GetValue());
      result = Value{SmallInt::Instance(), raw_val, nullptr, nullptr};
    }

    // Return result
    return result;
  }
};

//===----------------------------------------------------------------------===//
// TYPE SYSTEM CONSTRUCTION
//===----------------------------------------------------------------------===//

// The list of types a SQL smallint type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::SMALLINT, peloton::type::TypeId::INTEGER,
    peloton::type::TypeId::BIGINT, peloton::type::TypeId::DECIMAL};

// Explicit casting rules
static CastSmallInt kCastSmallInt;
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::BOOLEAN,
     kCastSmallInt},
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::TINYINT,
     kCastSmallInt},
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::SMALLINT,
     kCastSmallInt},
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::INTEGER,
     kCastSmallInt},
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::BIGINT,
     kCastSmallInt},
    {peloton::type::TypeId::SMALLINT, peloton::type::TypeId::DECIMAL,
     kCastSmallInt}};

// Comparison operations
static CompareSmallInt kCompareSmallInt;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareSmallInt}};

// Unary operators
static Negate kNegOp;
static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {
    {OperatorId::Negation, kNegOp}};

// Binary operations
static Add kAddOp;
static Sub kSubOp;
static Mul kMulOp;
static Div kDivOp;
static Modulo kModuloOp;
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::Add, kAddOp},
    {OperatorId::Sub, kSubOp},
    {OperatorId::Mul, kMulOp},
    {OperatorId::Div, kDivOp},
    {OperatorId::Mod, kModuloOp}};

}  // anonymous namespace

// Initialize the SMALLINT SQL type with the configured type system
SmallInt::SmallInt()
    : SqlType(peloton::type::TypeId::SMALLINT),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

Value SmallInt::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value SmallInt::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value SmallInt::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const16(peloton::type::PELOTON_INT16_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void SmallInt::GetTypeForMaterialization(CodeGen &codegen,
                                         llvm::Type *&val_type,
                                         llvm::Type *&len_type) const {
  val_type = codegen.Int16Type();
  len_type = nullptr;
}

llvm::Function *SmallInt::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton