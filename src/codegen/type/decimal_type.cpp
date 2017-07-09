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
// We do DECIMAL -> {INTEGRAL_TYPE, VARCHAR, BOOLEAN}
//===----------------------------------------------------------------------===//

struct CastDecimal : public TypeSystem::Cast {
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
  Value DoCast(CodeGen &codegen, const Value &value,
               const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

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

    return Value{to_type, result, nullptr, nullptr};
  }
};

// Comparison
struct CompareDecimal : public TypeSystem::Comparison {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Decimal::Instance() && left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpULT(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpULE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUEQ(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUNE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUGT(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    auto *raw_val = codegen->CreateFCmpUGE(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    llvm::Value *diff = codegen->CreateSub(left.GetValue(), right.GetValue());
    return Value{Integer::Instance(),
                 codegen->CreateFPToSI(diff, codegen.Int32Type()), nullptr,
                 nullptr};
  }
};

// Negation
struct Negate : public TypeSystem::UnaryOperator {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Decimal::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &val) const override {
    PL_ASSERT(SupportsType(val.GetType()));

    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        codegen.ConstDouble(0.0), val.GetValue(), overflow_bit);

    codegen.ThrowIfOverflow(overflow_bit);

    // Return result
    return Value{Decimal::Instance(), result, nullptr, nullptr};
  }
};

// Addition
struct Add : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFAdd(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Subtraction
struct Sub : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFSub(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Multiplication
struct Mul : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    auto *raw_val = codegen->CreateFMul(left.GetValue(), right.GetValue());
    return Value{Decimal::Instance(), raw_val, nullptr, nullptr};
  }
};

// Division
struct Div : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    llvm::Value *div0 =
        codegen->CreateFCmpUEQ(right.GetValue(), codegen.ConstDouble(0.0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{Decimal::Instance()};

    if (on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0};
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

    } else if (on_error == OnError::Exception) {
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
struct Modulo : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Decimal::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    llvm::Value *div0 =
        codegen->CreateFCmpUEQ(right.GetValue(), codegen.ConstDouble(0.0));

    // Check if the caller cares about division-by-zero errors

    auto result = Value{Decimal::Instance()};

    if (on_error == OnError::ReturnNull) {
      Value default_val, division_result;
      lang::If is_div0{codegen, div0};
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

    } else if (on_error == OnError::Exception) {
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

//===----------------------------------------------------------------------===//
// TYPE SYSTEM CONSTRUCTION
//===----------------------------------------------------------------------===//

// The list of types a SQL decimal type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::DECIMAL};

// Explicit casting rules
static CastDecimal kCastDecimal;
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
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
static CompareDecimal kCompareDecimal;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareDecimal}};

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

//===----------------------------------------------------------------------===//
// TINYINT TYPE CONFIGURATION
//===----------------------------------------------------------------------===//

// Initialize the DECIMAL SQL type with the configured type system
Decimal::Decimal()
    : SqlType(peloton::type::TypeId::DECIMAL),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

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
  return ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton