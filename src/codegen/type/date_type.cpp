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

//===----------------------------------------------------------------------===//
// Date casting rules
//
// We do DATE -> {TIMESTAMP, VARCHAR}
//===----------------------------------------------------------------------===//

struct CastDateToTimestamp : public TypeSystem::Cast {
  bool SupportsTypes(const type::Type &from_type,
                     const type::Type &to_type) const override {
    return from_type.GetSqlType() == Date::Instance() &&
           to_type.GetSqlType() == Timestamp::Instance();
  }

  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Date is number of days since 2000, timestamp is micros since same
    auto *date = codegen->CreateZExt(value.GetValue(), codegen.Int64Type());
    auto *usecs_per_date =
        codegen.Const64(peloton::type::TimestampType::kUsecsPerDate);
    llvm::Value *timestamp = codegen->CreateMul(date, usecs_per_date);

    return Value{to_type, timestamp, nullptr, nullptr};
  }
};

// Comparison
struct CompareDate : public TypeSystem::Comparison {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Date::Instance() && left_type == right_type;
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

// The list of types a SQL date type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP};

static CastDateToTimestamp kDateToTimestamp;
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP,
     kDateToTimestamp}};

static CompareDate kCompareDate;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareDate}};

static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

}  // anonymous namespace

// Initialize the DATE SQL type with the configured type system
Date::Date()
    : SqlType(peloton::type::TypeId::DATE),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

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
  return ValuesRuntimeProxy::_OutputDate::GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton