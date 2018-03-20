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

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
/// All right, this VARBINARY type is a giant hack. I admit it. It's copy-pasta
/// of VARCHAR. But, we don't have much support for it in the system overall, so
/// I'll leave the minimum required functionality here. When we start
/// implementing VARBINARY stuff, we will need to modify this.
///
////////////////////////////////////////////////////////////////////////////////

// Comparison
struct CompareVarbinary : public TypeSystem::ExpensiveComparisonHandleNull {
  bool SupportsTypes(const type::Type &left_type,
                     const type::Type &right_type) const override {
    return left_type.GetSqlType() == Varbinary::Instance() &&
           left_type == right_type;
  }

  // Call ValuesRuntime::CompareStrings(). This function behaves like strcmp(),
  // returning a values less than, equal to, or greater than zero if left is
  // found to be less than, matches, or is greater than the right value.
  llvm::Value *CompareStrings(CodeGen &codegen, const Value &left,
                              const Value &right) const {
    // Setup the function arguments and invoke the call
    std::vector<llvm::Value *> args = {left.GetValue(), left.GetLength(),
                                       right.GetValue(), right.GetLength()};
    return codegen.Call(ValuesRuntimeProxy::CompareStrings, args);
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is < 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_lt_0 = codegen->CreateICmpSLT(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_lt_0};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is <= 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_lte_0 = codegen->CreateICmpSLE(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_lte_0};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is == 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_eq_0 = codegen->CreateICmpEQ(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_eq_0};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is != 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_ne_0 = codegen->CreateICmpNE(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_ne_0};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is <= 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_gt_0 = codegen->CreateICmpSGT(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_gt_0};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, check is result is >= 0
    llvm::Value *result = CompareStrings(codegen, left, right);
    llvm::Value *is_gte_0 = codegen->CreateICmpSGE(result, codegen.Const32(0));
    return Value{Boolean::Instance(), is_gte_0};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                           const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    // Call CompareStrings, return result directly
    llvm::Value *result = CompareStrings(codegen, left, right);
    return Value{Integer::Instance(), result};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// Implicit casts
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::VARBINARY};

// Explicit casting rules
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {};

// Comparison operations
CompareVarbinary kCompareVarbinary;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {
    {kCompareVarbinary}};

// Unary operators
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {};

// Binary operations
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {};

// Nary operations
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

// NoArg operators
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// VARBINARY type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Varbinary::Varbinary()
    : SqlType(peloton::type::TypeId::VARBINARY),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable,
                   kBinaryOperatorTable, kNaryOperatorTable, 
                   kNoArgOperatorTable) {}

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