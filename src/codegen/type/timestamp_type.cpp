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

#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/date_type.h"
#include "codegen/type/integer_type.h"
#include "type/timestamp_type.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

//===----------------------------------------------------------------------===//
// Date casting rules
//
// We do DATE -> {TIMESTAMP, VARCHAR}
//===----------------------------------------------------------------------===//

struct CastTimestampToDate : public TypeSystem::Cast {
  bool SupportsTypes(const type::Type &from_type,
                     const type::Type &to_type) const override {
    return from_type.GetSqlType() == Timestamp::Instance() &&
           to_type.GetSqlType() == Date::Instance();
  }

  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               const type::Type &to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // TODO: Fix me
    auto *usecs_per_date =
        codegen.Const64(peloton::type::TimestampType::kUsecsPerDate);
    llvm::Value *date = codegen->CreateSDiv(value.GetValue(), usecs_per_date);
    llvm::Value *result = codegen->CreateTrunc(date, codegen.Int32Type());
    return Value{to_type, result, nullptr, nullptr};
  }
};

// Comparison
struct CompareTimestamp : public TypeSystem::Comparison {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type == Timestamp::Instance() && left_type == right_type;
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

// The list of types a SQL timestamp type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::DATE, peloton::type::TypeId::TIMESTAMP};

static CastTimestampToDate kTimestampToDate;
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {
    {peloton::type::TypeId::TIMESTAMP, peloton::type::TypeId::DATE,
     kTimestampToDate}};

static CompareTimestamp kCompareTimestamp;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareTimestamp}};

static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

}  // anonymous namespace

// Initialize the TIMESTAMP SQL type with the configured type system
Timestamp::Timestamp()
    : SqlType(peloton::type::TypeId::TIMESTAMP),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

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
  return ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton