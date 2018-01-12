//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_type.cpp
//
// Identification: src/codegen/type/timestamp_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/timestamp_type.h"

#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/proxy/date_functions_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/date_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/value.h"
#include "type/timestamp_type.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

////////////////////////////////////////////////////////////////////////////////
///
/// Casting
///
/// We do TIMESTAMP -> {TIMESTAMP, VARCHAR}
///
////////////////////////////////////////////////////////////////////////////////

struct CastTimestampToDate : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const type::Type &from_type,
                     const type::Type &to_type) const override {
    return from_type.GetSqlType() == Timestamp::Instance() &&
           to_type.GetSqlType() == Date::Instance();
  }

  // Cast the given decimal value into the provided type
  Value Impl(CodeGen &codegen, const Value &value,
             const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // TODO: Fix me
    auto *usecs_per_date =
        codegen.Const64(peloton::type::TimestampType::kUsecsPerDate);
    llvm::Value *date = codegen->CreateSDiv(value.GetValue(), usecs_per_date);
    llvm::Value *result = codegen->CreateTrunc(date, codegen.Int32Type());

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

struct CompareTimestamp : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Timestamp::Instance() && left_type == right_type;
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
/// No-argument operations
///
////////////////////////////////////////////////////////////////////////////////

struct Now : public TypeSystem::NoArgOperator {
  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Timestamp::Instance();
  }

  Value Eval(CodeGen &codegen,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    auto *raw_ret = codegen.Call(DateFunctionsProxy::Now, {});
    return Value{Timestamp::Instance(), raw_ret};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP};

// Explicit casts
CastTimestampToDate kTimestampToDate;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::TIMESTAMP, peloton::type::TypeId::DATE,
     kTimestampToDate}};

// Comparisons
CompareTimestamp kCompareTimestamp;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareTimestamp}};

// Unary operations
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// No-arg operations
Now kNow;
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {
    {OperatorId::Now, kNow}};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// TIMESTAMP type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Timestamp::Timestamp()
    : SqlType(peloton::type::TypeId::TIMESTAMP),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

Value Timestamp::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_TIMESTAMP_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Timestamp::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_TIMESTAMP_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Timestamp::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const64(peloton::type::PELOTON_TIMESTAMP_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void Timestamp::GetTypeForMaterialization(CodeGen &codegen,
                                          llvm::Type *&val_type,
                                          llvm::Type *&len_type) const {
  val_type = codegen.Int64Type();
  len_type = nullptr;
}

llvm::Function *Timestamp::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::OutputTimestamp.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
