//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varchar_type.cpp
//
// Identification: src/codegen/type/varchar_type.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/varchar_type.h"

#include "codegen/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

// Comparison
struct CompareVarchar : public TypeSystem::Comparison {
  bool SupportsTypes(const type::Type &left_type,
                     const type::Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           left_type == right_type;
  }

  // Call ValuesRuntime::CompareStrings(). This function behaves like strcmp(),
  // returning a values less than, equal to, or greater than zero if left is
  // found to be less than, matches, or is greater than the right value.
  llvm::Value *CallCompareStrings(CodeGen &codegen, const Value &left,
                                  const Value &right) const {
    // Get the proxy to ValuesRuntime::CompareStrings(...)
    auto *cmp_func = ValuesRuntimeProxy::_CompareStrings::GetFunction(codegen);

    // Setup the function arguments and invoke the call
    std::vector<llvm::Value *> args = {left.GetValue(), left.GetLength(),
                                       right.GetValue(), right.GetLength()};
    return codegen.CallFunc(cmp_func, args);
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is < 0
    result = codegen->CreateICmpSLT(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is <= 0
    result = codegen->CreateICmpSLE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpEQ(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpNE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is > 0
    result = codegen->CreateICmpSGT(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is >= 0
    result = codegen->CreateICmpSGE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Return the comparison
    return Value{Integer::Instance(), result};
  }
};

//===----------------------------------------------------------------------===//
// TYPE SYSTEM CONSTRUCTION
//===----------------------------------------------------------------------===//

// The list of types a SQL varchar type can be implicitly casted to
const std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::VARCHAR};

// Explicit casting rules
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {};

// Comparison operations
static CompareVarchar kCompareVarchar;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {false, kCompareVarchar}};

// Unary operators
static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

}  // anonymous namespace

//===----------------------------------------------------------------------===//
// TINYINT TYPE CONFIGURATION
//===----------------------------------------------------------------------===//

// Initialize the TINYINT SQL type with the configured type system
Varchar::Varchar()
    : SqlType(peloton::type::TypeId::VARCHAR),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable) {}

}  // namespace type
}  // namespace codegen
}  // namespace peloton