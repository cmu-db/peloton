//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varchar_type.cpp
//
// Identification: src/codegen/type/varchar_type.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/varchar_type.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/proxy/timestamp_functions_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/proxy/date_functions_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/timestamp_type.h"
#include "codegen/value.h"
#include "codegen/vector.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

////////////////////////////////////////////////////////////////////////////////
///
/// Comparisons
///
////////////////////////////////////////////////////////////////////////////////

struct CompareVarchar : public TypeSystem::ExpensiveComparisonHandleNull {
  bool SupportsTypes(const type::Type &left_type,
                     const type::Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
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
/// Unary operators
///
////////////////////////////////////////////////////////////////////////////////

// ASCII
struct Ascii : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Integer::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &val,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *raw_ret =
        codegen.Call(StringFunctionsProxy::Ascii,
                     {executor_ctx, val.GetValue(), val.GetLength()});
    return Value{Integer::Instance(), raw_ret};
  }
};

// Length
struct Length : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Integer::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &val,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *raw_ret =
        codegen.Call(StringFunctionsProxy::Length,
                     {executor_ctx, val.GetValue(), val.GetLength()});
    return Value{Integer::Instance(), raw_ret};
  }
};

// Trim
struct Trim : public TypeSystem::UnaryOperatorHandleNull {
  bool SupportsType(const Type &type) const override {
    return type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &val_type) const override {
    return Varchar::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &val,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *ret =
        codegen.Call(StringFunctionsProxy::Trim,
                     {executor_ctx, val.GetValue(), val.GetLength()});

    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Binary operators
///
////////////////////////////////////////////////////////////////////////////////

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

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const {
    // Call StringFunctions::Like(...)

    // Setup input arguments
    llvm::Value *executor_ctx = ctx.executor_context;
    std::vector<llvm::Value *> args = {executor_ctx, left.GetValue(),
                                       left.GetLength(), right.GetValue(),
                                       right.GetLength()};

    // Make call
    llvm::Value *raw_ret = codegen.Call(StringFunctionsProxy::Like, args);

    // Return the result
    return Value{Boolean::Instance(), raw_ret};
  }

  Value Eval(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Pre-condition: Left value is the input string; right value is the pattern

    if (!left.IsNullable()) {
      return Impl(codegen, left, right, ctx);
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
      not_null_ret = Impl(codegen, left, right, ctx);
    }
    return input_null.BuildPHI(null_ret, not_null_ret);
  }
};

// DateTrunc
// TODO(lma): Move this to the Timestamp type once the function lookup logic is
// corrected.
struct DateTrunc : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Timestamp::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Timestamp::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    llvm::Value *raw_ret = codegen.Call(TimestampFunctionsProxy::DateTrunc,
                                        {left.GetValue(), right.GetValue()});
    return Value{Timestamp::Instance(), raw_ret};
  }
};

struct DatePart : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Timestamp::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Type{Decimal::Instance()};
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             UNUSED_ATTRIBUTE const TypeSystem::InvocationContext &ctx)
      const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    llvm::Value *raw_ret = codegen.Call(TimestampFunctionsProxy::DatePart,
                                        {left.GetValue(), right.GetValue()});
    return Value{Decimal::Instance(), raw_ret};
  }
};

struct BTrim : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Varchar::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *ret =
        codegen.Call(StringFunctionsProxy::BTrim,
                     {executor_ctx, left.GetValue(), left.GetLength(),
                      right.GetValue(), right.GetLength()});

    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

struct LTrim : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Varchar::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *ret =
        codegen.Call(StringFunctionsProxy::LTrim,
                     {executor_ctx, left.GetValue(), left.GetLength(),
                      right.GetValue(), right.GetLength()});

    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

struct RTrim : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Varchar::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Varchar::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *executor_ctx = ctx.executor_context;
    llvm::Value *ret =
        codegen.Call(StringFunctionsProxy::RTrim,
                     {executor_ctx, left.GetValue(), left.GetLength(),
                      right.GetValue(), right.GetLength()});

    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

struct Repeat : public TypeSystem::BinaryOperatorHandleNull {
  bool SupportsTypes(const Type &left_type,
                     const Type &right_type) const override {
    return left_type.GetSqlType() == Varchar::Instance() &&
           right_type.GetSqlType() == Integer::Instance();
  }

  Type ResultType(UNUSED_ATTRIBUTE const Type &left_type,
                  UNUSED_ATTRIBUTE const Type &right_type) const override {
    return Varchar::Instance();
  }

  Value Impl(CodeGen &codegen, const Value &left, const Value &right,
             const TypeSystem::InvocationContext &ctx) const override {
    llvm::Value *ret = codegen.Call(StringFunctionsProxy::Repeat,
                                    {ctx.executor_context, left.GetValue(),
                                     left.GetLength(), right.GetValue()});

    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

/**
 * 15-721 Spring 2018
 * You should uncomment the following struct once you have created
 * the catalog and StringFunctions implementation.
 */
// struct Concat : public TypeSystem::NaryOperator,
//                public TypeSystem::BinaryOperator {
//  bool SupportsTypes(const std::vector<Type> &arg_types) const override {
//    // Every input must be a string
//    for (const auto &type : arg_types) {
//      if (type.GetSqlType() != Varchar::Instance()) {
//        return false;
//      }
//    }
//    return true;
//  }
//
//  bool SupportsTypes(const Type &left_type,
//                     const Type &right_type) const override {
//    return SupportsTypes({left_type, right_type});
//  }
//
//  Type ResultType(
//      UNUSED_ATTRIBUTE const std::vector<Type> &arg_types) const override {
//    return Varchar::Instance();
//  }
//
//  Type ResultType(const Type &left_type,
//                  const Type &right_type) const override {
//    return ResultType({left_type, right_type});
//  }
//
//  Value Eval(CodeGen &codegen, const std::vector<Value> &input_args,
//             const TypeSystem::InvocationContext &ctx) const override {
//    // Make room on stack to store each of the input strings and their lengths
//    auto num_inputs = static_cast<uint32_t>(input_args.size());
//    auto *concat_str_buffer =
//        codegen.AllocateBuffer(codegen.CharPtrType(), num_inputs,
//        "concatStrs");
//    auto *concat_str_lens_buffer = codegen.AllocateBuffer(
//        codegen.Int32Type(), num_inputs, "concatStrLens");
//
//    // Create vector accessors to simplify creating the store instructions
//    Vector concat_strs{concat_str_buffer, num_inputs, codegen.CharPtrType()};
//    Vector concat_strs_lens{concat_str_lens_buffer, num_inputs,
//                            codegen.Int32Type()};
//
//    // Store each input string into the on-stack buffer
//    for (uint32_t i = 0; i < input_args.size(); i++) {
//      auto *index = codegen.Const32(i);
//      concat_strs.SetValue(codegen, index, input_args[i].GetValue());
//      concat_strs_lens.SetValue(codegen, index, input_args[i].GetLength());
//    }
//
//    // Setup the input arguments for the final function call
//    std::vector<llvm::Value *> func_args = {
//        ctx.executor_context, concat_strs.GetVectorPtr(),
//        concat_strs_lens.GetVectorPtr(), codegen.Const32(num_inputs)};
//
//    // Invoke StringFunctions::Concat(...)
//    auto *ret = codegen.Call(StringFunctionsProxy::Concat, func_args);
//
//    // Pull out what we need and return
//    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
//    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
//    return Value{Varchar::Instance(), str_ptr, str_len};
//  }
//
//  Value Eval(CodeGen &codegen, const Value &left, const Value &right,
//             const TypeSystem::InvocationContext &ctx) const override {
//    return Eval(codegen, {left, right}, ctx);
//  }
//};

////////////////////////////////////////////////////////////////////////////////
///
/// N-ary operators
///
////////////////////////////////////////////////////////////////////////////////

struct Substr : public TypeSystem::NaryOperator {
  // The first argument is the original string
  // The second argument is the starting offset of the substring
  // The third argument is the length of the substring
  bool SupportsTypes(const std::vector<Type> &arg_types) const override {
    return arg_types[0].GetSqlType() == Varchar::Instance() &&
           arg_types[1].GetSqlType() == Integer::Instance() &&
           arg_types[2].GetSqlType() == Integer::Instance();
  }

  Type ResultType(
      UNUSED_ATTRIBUTE const std::vector<Type> &arg_types) const override {
    return Varchar::Instance();
  }

  Value Eval(CodeGen &codegen, const std::vector<Value> &input_args,
             const TypeSystem::InvocationContext &ctx) const override {
    // Setup function arguments
    llvm::Value *executor_ctx = ctx.executor_context;
    std::vector<llvm::Value *> args = {
        executor_ctx,
        input_args[0].GetValue(),
        input_args[0].GetLength(),
        input_args[1].GetValue(),
        input_args[2].GetValue(),
    };

    // Call
    llvm::Value *ret = codegen.Call(StringFunctionsProxy::Substr, args);

    // Pull out what we need and return
    llvm::Value *str_ptr = codegen->CreateExtractValue(ret, 0);
    llvm::Value *str_len = codegen->CreateExtractValue(ret, 1);
    return Value{Varchar::Instance(), str_ptr, str_len};
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Function tables
///
////////////////////////////////////////////////////////////////////////////////

// The list of types a SQL varchar type can be implicitly casted to
std::vector<peloton::type::TypeId> kImplicitCastingTable = {
    peloton::type::TypeId::VARCHAR};

// Explicit casting rules
std::vector<TypeSystem::CastInfo> kExplicitCastingTable = {};

// Comparison operations
CompareVarchar kCompareVarchar;
std::vector<TypeSystem::ComparisonInfo> kComparisonTable = {{kCompareVarchar}};

// Unary operators
Ascii kAscii;
Length kLength;
Trim kTrim;
std::vector<TypeSystem::UnaryOpInfo> kUnaryOperatorTable = {
    {OperatorId::Ascii, kAscii},
    {OperatorId::Length, kLength},
    {OperatorId::Trim, kTrim}};

// Binary operations
Like kLike;
DateTrunc kDateTrunc;
DatePart kDatePart;
BTrim kBTrim;
LTrim kLTrim;
RTrim kRTrim;
Repeat kRepeat;
std::vector<TypeSystem::BinaryOpInfo> kBinaryOperatorTable = {
    {OperatorId::Like, kLike},         {OperatorId::DateTrunc, kDateTrunc},
    {OperatorId::DatePart, kDatePart}, {OperatorId::BTrim, kBTrim},
    {OperatorId::LTrim, kLTrim},       {OperatorId::RTrim, kRTrim},
    {OperatorId::Repeat, kRepeat}};

// Nary operations
Substr kSubstr;
std::vector<TypeSystem::NaryOpInfo> kNaryOperatorTable = {
    {OperatorId::Substr, kSubstr}};

// NoArg operators
std::vector<TypeSystem::NoArgOpInfo> kNoArgOperatorTable = {};

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// VARCHAR type initialization and configuration
///
////////////////////////////////////////////////////////////////////////////////

Varchar::Varchar()
    : SqlType(peloton::type::TypeId::VARCHAR),
      type_system_(kImplicitCastingTable, kExplicitCastingTable,
                   kComparisonTable, kUnaryOperatorTable, kBinaryOperatorTable,
                   kNaryOperatorTable, kNoArgOperatorTable) {}

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
