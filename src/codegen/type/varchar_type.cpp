//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varchar_type.cpp
//
// Identification: src/codegen/type/varchar_type.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/varchar_type.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/timestamp_type.h"
#include "codegen/proxy/timestamp_functions_proxy.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

// Comparison
struct CompareVarchar : public TypeSystem::SimpleNullableComparison {
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
    // Setup the function arguments and invoke the call
    std::vector<llvm::Value *> args = {left.GetValue(), left.GetLength(),
                                       right.GetValue(), right.GetLength()};
    return codegen.Call(ValuesRuntimeProxy::CompareStrings, args);
  }

  Value CompareLtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is < 0
    result = codegen->CreateICmpSLT(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareLteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is <= 0
    result = codegen->CreateICmpSLE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareEqImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpEQ(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareNeImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpNE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareGtImpl(CodeGen &codegen, const Value &left,
                      const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is > 0
    result = codegen->CreateICmpSGT(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareGteImpl(CodeGen &codegen, const Value &left,
                       const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is >= 0
    result = codegen->CreateICmpSGE(result, codegen.Const32(0));

    // Return the comparison
    return Value{Boolean::Instance(), result};
  }

  Value CompareForSortImpl(CodeGen &codegen, const Value &left,
                           const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Return the comparison
    return Value{Integer::Instance(), result};
  }
};

struct Ascii : public TypeSystem::SimpleNullableUnaryOperator {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Integer::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &val) const override {
    llvm::Value *raw_ret = codegen.Call(StringFunctionsProxy::Ascii,
                                        {val.GetValue(), val.GetLength()});
    return Value{Integer::Instance(), raw_ret};
  }
};

struct Like : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           left_type == right_type;
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Boolean::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right) const {
    // Call StringFunctions::Like(...)
    llvm::Value *raw_ret = codegen.Call(StringFunctionsProxy::Like,
                                        {left.GetValue(), left.GetLength(),
                                         right.GetValue(), right.GetLength()});
    // Return the result
    return Value{Boolean::Instance(), raw_ret};
  }

  Value Eval(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));
    
    // Pre-condition: Left value is the input string; right value is the pattern
    
    if (!left.IsNullable()) {
      return Impl(codegen, left, right);
    }

    codegen::Value null_ret, not_null_ret;
    lang::If input_null{codegen, left.IsNull(codegen)};
    {
      // Input is null, return false
      null_ret = codegen::Value{Boolean::Instance(), codegen.ConstBool(false)};
    }
    input_null.ElseBlock();
    { 
      // Input is not null, invoke LIKE
      not_null_ret = Impl(codegen, left, right); 
    }
    return input_null.BuildPHI(null_ret, not_null_ret);
  }
};

struct Length : public TypeSystem::UnaryOperator {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Integer::Instance();
  }

  Value DoWork(CodeGen &codegen, const Value &val) const override {
    llvm::Value *raw_ret = codegen.Call(StringFunctionsProxy::Length,
                                        {val.GetValue(), val.GetLength()});
    return Value{Integer::Instance(), raw_ret};
  }
};

// DateTrunc
// TODO(lma): Move this to the Timestamp type once the function lookup logic is
// corrected.
struct DateTrunc : public TypeSystem::BinaryOperator {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Timestamp::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Timestamp::Instance()};
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    llvm::Value *raw_ret = codegen.Call(TimestampFunctionsProxy::DateTrunc,
                                        {left.GetValue(), right.GetValue()});
    return Value{Timestamp::Instance(), raw_ret};
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
    {kCompareVarchar}};

// Unary operators
static Ascii kAscii;
static Length kLength;
static std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {
    {OperatorId::Ascii, kAscii}, {OperatorId::Length, kLength}};

// Binary operations
static Like kLike;
static DateTrunc kDateTrunc;
static std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::Like, kLike}, {OperatorId::DateTrunc, kDateTrunc}};

// Nary operations
static std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {};

}  // anonymous namespace

//===----------------------------------------------------------------------===//
// TINYINT TYPE CONFIGURATION
//===----------------------------------------------------------------------===//

// Initialize the VARCHAR SQL type with the configured type system
Varchar::Varchar()
    : SqlType(peloton::type::TypeId::VARCHAR),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable) {}

Value Varchar::GetMinValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"The VARCHAR type does not have a minimum value ..."};
}

Value Varchar::GetMaxValue(UNUSED_ATTRIBUTE CodeGen &codegen) const {
  throw Exception{"The VARCHAR type does not have a maximum value ..."};
}

Value Varchar::GetNullValue(CodeGen &codegen) const {
  return Value{Type{TypeId(), true}, codegen.NullPtr(codegen.CharPtrType()),
               codegen.Const32(0), codegen.ConstBool(true)};
}

void Varchar::GetTypeForMaterialization(CodeGen &codegen, llvm::Type *&val_type,
                                        llvm::Type *&len_type) const {
  val_type = codegen.CharPtrType();
  len_type = codegen.Int32Type();
}

llvm::Function *Varchar::GetOutputFunction(
    CodeGen &codegen, UNUSED_ATTRIBUTE const Type &type) const {
  // TODO: We should use the length information in the type?
  return ValuesRuntimeProxy::OutputVarchar.GetFunction(codegen);
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
