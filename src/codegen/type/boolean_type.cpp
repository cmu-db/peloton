//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_type.cpp
//
// Identification: src/codegen/type/boolean_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/boolean_type.h"

#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/varchar_type.h"
#include "codegen/value.h"
#include "common/exception.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

////////////////////////////////////////////////////////////////////////////////
///
/// Casting
///
////////////////////////////////////////////////////////////////////////////////

struct CastBooleanToInteger : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    return from_type.type_id == peloton::type::TypeId::BOOLEAN &&
           to_type.type_id == peloton::type::TypeId::INTEGER;
  }

  Value Impl(CodeGen &codegen, const Value &value,
             const Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Any integral value requires a zero-extension
    auto *raw_val = codegen->CreateZExt(value.GetValue(), codegen.Int32Type());

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    // Return the result
    return Value{to_type, raw_val, nullptr, null};
  }
};

struct CastBooleanToDecimal : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    return from_type.type_id == peloton::type::TypeId::BOOLEAN &&
           to_type.type_id == peloton::type::TypeId::DECIMAL;
  }

  Value Impl(CodeGen &codegen, const Value &value,
             const Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Converts True to 1.0 and False to 0.0
    auto *raw_val =
        codegen->CreateUIToFP(value.GetValue(), codegen.DoubleType());
    return Value{to_type, raw_val, nullptr, nullptr};
  }
};

struct CastBooleanToVarchar : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    return from_type.type_id == peloton::type::TypeId::BOOLEAN &&
           to_type.type_id == peloton::type::TypeId::VARCHAR;
  }

  Value Impl(CodeGen &codegen, const Value &value,
             const Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Convert this boolean (unsigned int) into a string
    llvm::Value *str_val = codegen->CreateSelect(
        value.GetValue(), codegen.ConstString("T"), codegen.ConstString("F"));

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    // Return the result
    return Value{to_type, str_val, codegen.Const32(1), null};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

struct CompareBoolean : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.type_id == peloton::type::TypeId::BOOLEAN &&
           left_type == right_type;
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                           const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // For boolean sorting, we convert 1-bit boolean values into a 32-bit number
    const auto int_type = type::Type{Integer::Instance()};
    Value casted_left = left.CastTo(codegen, int_type);
    Value casted_right = right.CastTo(codegen, int_type);

    return casted_left.Sub(codegen, casted_right);
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Binary operations
///
////////////////////////////////////////////////////////////////////////////////

// Logical AND
struct LogicalAnd : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Boolean::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return type::Type{Boolean::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    auto *raw_val = codegen->CreateAnd(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }
};

// Logical OR
struct LogicalOr : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Boolean::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return type::Type{Boolean::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    auto *raw_val = codegen->CreateOr(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::BOOLEAN};


// Explicit casts
CastBooleanToInteger kBooleanToInteger;
CastBooleanToDecimal kBooleanToDecimal;
CastBooleanToVarchar kBooleanToVarchar;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::BOOLEAN, peloton::type::TypeId::INTEGER,
     kBooleanToInteger},
    {peloton::type::TypeId::BOOLEAN, peloton::type::TypeId::VARCHAR,
     kBooleanToVarchar},
    {peloton::type::TypeId::BOOLEAN, peloton::type::TypeId::DECIMAL,
     kBooleanToDecimal}};

// Comparison operations
CompareBoolean kCompareBoolean;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareBoolean}};

// Unary operations
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
LogicalAnd kLogicalAnd;
LogicalOr kLogicalOr;
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::LogicalAnd, kLogicalAnd}, {OperatorId::LogicalOr, kLogicalOr}};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// No arg operations
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// BOOLEAN type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Boolean::Boolean()
    : SqlType(peloton::type::TypeId::BOOLEAN),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

Value Boolean::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Boolean::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Boolean::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.ConstBool(peloton::type::PELOTON_BOOLEAN_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

llvm::Value *Boolean::CheckNull(CodeGen &codegen, llvm::Value *bool_ptr) const {
  auto *b = codegen->CreateLoad(
      codegen.Int8Type(),
      codegen->CreateBitCast(bool_ptr, codegen.Int8Type()->getPointerTo()));
  return codegen->CreateICmpEQ(
      b, codegen.Const8(peloton::type::PELOTON_BOOLEAN_NULL));
}

void Boolean::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                        llvm::Type *&len_type) const {
  val_type = codegen.BoolType();
  len_type = nullptr;
}

llvm::Function *Boolean::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::OutputBoolean.GetFunction(codegen);
}

// This method reifies a NULL-able boolean value, thanks to the weird-ass
// three-valued logic in SQL. The logic adheres to the table below:
//
//   INPUT | OUTPUT
// +-------+--------+
// | false | false  |
// +-------+--------+
// | null  | false  |
// +-------+--------+
// | true  | true   |
// +-------+--------+
//
llvm::Value *Boolean::Reify(CodeGen &codegen, const Value &bool_val) const {
  if (!bool_val.IsNullable()) {
    return bool_val.GetValue();
  } else {
    return codegen->CreateSelect(bool_val.IsNull(codegen),
                                 codegen.ConstBool(false), bool_val.GetValue());
  }
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton