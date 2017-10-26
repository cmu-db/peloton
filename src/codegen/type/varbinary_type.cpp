//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varbinary_type.cpp
//
// Identification: src/codegen/type/varbinary_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/varbinary_type.h"

#include "codegen/value.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "common/exception.h"
#include "type/limits.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

///===--------------------------------------------------------------------===///
///
/// All right, this VARBINARY type is a giant hack. I admit it. It's copy-pasta
/// of VARCHAR. But, we don't have much support for it in the system overall, so
/// I'll leave the minimum required functionality here. When we start
/// implementing VARBINARY stuff, we will need to modify this.
///
///===--------------------------------------------------------------------===///

// Comparison
struct CompareVarbinary : public TypeSystem::Comparison {
  bool SupportsTypes(const type::Type &left_type,
                     const type::Type &right_type) const override {
    return left_type.GetSqlType() == Varbinary::Instance() &&
           left_type == right_type;
  }

  // Call ValuesRuntime::CompareStrings(). This function behaves like strcmp(),
  // returning a values less than, equal to, or greater than zero if left is
  // found to be less than, matches, or is greater than the right value.
  llvm::Value *CallCompareStrings(CodeGen &codegen, const Value &left,
                                  const Value &right) const {
    // Setup the function arguments and invoke the call
    std::vector<llvm::Value *> args = {left.GetValue(), left.GetLength(),
                                       right.GetValue(), right.GetLength()};
    return codegen.Call(ValuesRuntimeProxy::CompareStrings, args);
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
    peloton::type::TypeId::VARBINARY};

// Explicit casting rules
static std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {};

// Comparison operations
static CompareVarbinary kCompareVarbinary;
static std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareVarbinary}};

// Unary operators
static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

// Nary operations
static std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

}  // anonymous namespace

//===----------------------------------------------------------------------===//
// VARBINARY TYPE CONFIGURATION
//===----------------------------------------------------------------------===//

// Initialize the VARBINARY/BLOB SQL type with the configured type system
Varbinary::Varbinary()
    : SqlType(peloton::type::TypeId::VARBINARY),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable, kNaryOperatorTable) {}

Value Varbinary::GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"The VARBINARY type does not have a minimum value ..."};
}

Value Varbinary::GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"The VARBINARY type does not have a maximum value ..."};
}

Value Varbinary::GetNullValue(CodeGen &codegen) const {
  return Value{Type{TypeId(), true}, codegen.NullPtr(codegen.CharPtrType()),
               codegen.Const32(0), codegen.ConstBool(true)};
}

void Varbinary::GetTypeForMaterialization(CodeGen &codegen,
                                          llvm::Type *&val_type,
                                          llvm::Type *&len_type) const {
  val_type = codegen.CharPtrType();
  len_type = codegen.Int32Type();
}

llvm::Function *Varbinary::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  // TODO: We should use the length information in the type?
  return ValuesRuntimeProxy::OutputVarbinary.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton