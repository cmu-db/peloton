//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_type.cpp
//
// Identification: src/codegen/type/date_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/date_type.h"

#include "codegen/lang/if.h"
#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/timestamp_type.h"
#include "type/timestamp_type.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

////////////////////////////////////////////////////////////////////////////////
///
/// Casting rules
///
/// We do DATE -> {TIMESTAMP, VARCHAR}
///
////////////////////////////////////////////////////////////////////////////////

struct CastDateToTimestamp : public TypeSystem::CastHandleNull {
  bool SupportsTypes(const type::Type &from_type,
                     const type::Type &to_type) const override {
    return from_type.GetSqlType() == Date::Instance() &&
           to_type.GetSqlType() == Timestamp::Instance();
  }

  // Cast the given decimal value into the provided type
  Value Impl(CodeGen &codegen, const Value &value,
             const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Date is number of days since 2000, timestamp is micros since same
    auto *date = codegen->CreateZExt(value.GetValue(), codegen.Int64Type());
    auto *usecs_per_date =
        codegen.Const64(peloton::type::TimestampType::kUsecsPerDate);
    llvm::Value *timestamp = codegen->CreateMul(date, usecs_per_date);

    // We could be casting this non-nullable value to a nullable type
    llvm::Value *null = to_type.nullable ? codegen.ConstBool(false) : nullptr;

    return Value{to_type, timestamp, nullptr, null};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

// Comparison
struct CompareDate : public TypeSystem::SimpleComparisonHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Date::Instance() && left_type == right_type;
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
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP};

// Explicit casts
CastDateToTimestamp kDateToTimestamp;
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP,
     kDateToTimestamp}};

// Comparison operations
CompareDate kCompareDate;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareDate}};

// Unary operations
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// No arg operations
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// DATE type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Date::Date()
    : SqlType(peloton::type::TypeId::DATE),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

Value Date::GetMinValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const32(peloton::type::PELOTON_DATE_MIN);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Date::GetMaxValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const32(peloton::type::PELOTON_DATE_MAX);
  return Value{*this, raw_val, nullptr, nullptr};
}

Value Date::GetNullValue(CodeGen &codegen) const {
  auto *raw_val = codegen.Const32(peloton::type::PELOTON_DATE_NULL);
  return Value{Type{TypeId(), true}, raw_val, nullptr, codegen.ConstBool(true)};
}

void Date::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                     llvm::Type *&len_type) const {
  val_type = codegen.Int32Type();
  len_type = nullptr;
}

llvm::Function *Date::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  return ValuesRuntimeProxy::OutputDate.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton