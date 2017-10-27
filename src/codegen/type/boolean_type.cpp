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

#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/varchar_type.h"
#include "common/exception.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

//===----------------------------------------------------------------------===//
// CASTING OPERATIONS
//===----------------------------------------------------------------------===//

struct CastBooleanToInteger : public TypeSystem::Cast {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    return from_type.type_id == peloton::type::TypeId::BOOLEAN &&
           to_type.type_id == peloton::type::TypeId::INTEGER;
  }

  Value DoCast(CodeGen &codegen, const Value &value,
               const Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Any integral value requires a zero-extension
    auto *raw_val = codegen->CreateZExt(value.GetValue(), codegen.Int32Type());
    return Value{to_type, raw_val, nullptr, nullptr};
  }
};

struct CastBooleanToVarchar : public TypeSystem::Cast {
  bool SupportsTypes(const Type &from_type,
                     const Type &to_type) const override {
    return from_type.type_id == peloton::type::TypeId::BOOLEAN &&
           to_type.type_id == peloton::type::TypeId::VARCHAR;
  }

  Value DoCast(CodeGen &codegen, const Value &value,
               const Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Convert this boolean (unsigned int) into a string
    llvm::Value *str_val = codegen->CreateSelect(
        value.GetValue(), codegen.ConstString("T"), codegen.ConstString("F"));
    return Value{to_type, str_val, codegen.Const32(1)};
  }
};

//===----------------------------------------------------------------------===//
// COMPARISON
//===----------------------------------------------------------------------===//

// Comparison functions
struct CompareBoolean : public TypeSystem::Comparison {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.type_id == peloton::type::TypeId::BOOLEAN &&
           left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{Boolean::Instance(), result, nullptr, nullptr};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // For boolean sorting, we convert 1-bit boolean values into a 32-bit number
    const auto int_type = type::Type{Integer::Instance()};
    Value casted_left = left.CastTo(codegen, int_type);
    Value casted_right = right.CastTo(codegen, int_type);

    return casted_left.Sub(codegen, casted_right);
  }
};

//===----------------------------------------------------------------------===//
// BINARY OPERATIONS
//===----------------------------------------------------------------------===//

// Logical AND
struct LogicalAnd : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Boolean::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return type::Type{Boolean::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    auto *raw_val = codegen->CreateAnd(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }
};

// Logical OR
struct LogicalOr : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Boolean::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return type::Type{Boolean::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    auto *raw_val = codegen->CreateOr(left.GetValue(), right.GetValue());
    return Value{Boolean::Instance(), raw_val, nullptr, nullptr};
  }
};

//===----------------------------------------------------------------------===//
// TYPE SYSTEM CONSTRUCTION
//===----------------------------------------------------------------------===//

// The list of types a SQL boolean type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::BOOLEAN};

static CastBooleanToInteger kBooleanToInteger;
static CastBooleanToVarchar kBooleanToVarchar;
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::BOOLEAN, peloton::type::TypeId::INTEGER,
     kBooleanToInteger},
    {peloton::type::TypeId::BOOLEAN, peloton::type::TypeId::VARCHAR,
     kBooleanToVarchar}};

static CompareBoolean kCompareBoolean;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareBoolean}};

static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

static LogicalAnd kLogicalAnd;
static LogicalOr kLogicalOr;
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::LogicalAnd, kLogicalAnd}, {OperatorId::LogicalOr, kLogicalOr}};

// Nary operations
static std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

}  // anonymous namespace

// Initialize the BOOLEAN SQL type with the configured type system
Boolean::Boolean()
    : SqlType(peloton::type::TypeId::BOOLEAN),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable, kNaryOperatorTable) {}

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