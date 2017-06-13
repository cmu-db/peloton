//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type.cpp
//
// Identification: src/codegen/type.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type.h"

#include "codegen/if.h"
#include "codegen/values_runtime_proxy.h"
#include "type/timestamp_type.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

///===--------------------------------------------------------------------===///
/// CASTING RULES
/// TODO: Implement other casting rules
///===--------------------------------------------------------------------===///

class CastWithNullPropagation : public Type::Cast {
 public:
  CastWithNullPropagation(Type::Cast &inner_cast) : inner_cast_(inner_cast) {}

  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return inner_cast_.SupportsTypes(from_type, to_type);
  }

  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    // Do the cast
    Value ret = inner_cast_.DoCast(codegen, value, to_type);

    // Return the value with the null-bit propagated from the input
    return Value{ret.GetType(), ret.GetValue(), ret.GetLength(),
                 value.GetNullBit()};
  }

 private:
  // The inner (non-null-aware) cast operation
  Type::Cast &inner_cast_;
};

//===----------------------------------------------------------------------===//
// Boolean casting rules
//
// Right now, we only support BOOL -> {INTEGER, VARCHAR} casts. This is mostly
// because this is the default in Postgres.
//===----------------------------------------------------------------------===//

struct CastBoolean : public Type::Cast {
  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return from_type == type::TypeId::BOOLEAN &&
           (to_type == type::TypeId::INTEGER ||
            to_type == type::TypeId::VARCHAR);
  }

  // Cast the boolean value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    // This function only does boolean -> int casting
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    if (to_type == type::TypeId::INTEGER) {
      // Any integral value requires a zero-extension
      return Value{to_type,
                   codegen->CreateZExt(value.GetValue(), codegen.Int32Type())};
    } else if (to_type == type::TypeId::VARCHAR) {
      // Convert this boolean (unsigned int) into a string
      llvm::Value *str_val = codegen->CreateSelect(
          value.GetValue(), codegen.ConstString("T"), codegen.ConstString("F"));
      return Value{to_type, str_val, codegen.Const32(1)};
    }

    // TODO: Fill me out
    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

//===----------------------------------------------------------------------===//
// Integer (8-, 16-, 32-, 64-bit) casting rules
//
// We do INTEGRAL_TYPE -> {INTEGRAL_TYPE, DECIMAL, VARCHAR, BOOLEAN}
//===----------------------------------------------------------------------===//

struct CastInteger : public Type::Cast {
  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return Type::IsIntegral(from_type) &&
           (Type::IsIntegral(to_type) || Type::IsNumeric(to_type) ||
            to_type == type::TypeId::VARCHAR ||
            to_type == type::TypeId::BOOLEAN);
  }

  // Cast the given integer value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (to_type == type::TypeId::BOOLEAN || Type::IsIntegral(to_type)) {
      // For integral casts, we need to either truncate or sign-extend
      uint32_t curr_size = Type::GetFixedSizeForType(value.GetType());
      uint32_t target_size = Type::GetFixedSizeForType(to_type);

      llvm::Value *res = nullptr;
      if (curr_size < target_size) {
        res = codegen->CreateSExt(value.GetValue(), val_type);
      } else {
        res = codegen->CreateTrunc(value.GetValue(), val_type);
      }

      // We're done
      return Value{to_type, res};
    } else if (Type::IsNumeric(to_type)) {
      // Convert this integral value into a floating point double
      llvm::Value *res = codegen->CreateSIToFP(value.GetValue(), val_type);

      // We're done
      return Value{to_type, res};
    } else if (to_type == type::TypeId::VARCHAR) {
      // TODO: Implement me
    }

    // TODO: Fill me out
    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

//===----------------------------------------------------------------------===//
// Decimal casting rules
//
// We do DECIMAL -> {INTEGRAL_TYPE, DECIMAL, VARCHAR, BOOLEAN}
//===----------------------------------------------------------------------===//

struct CastDecimal : public Type::Cast {
  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return Type::IsNumeric(from_type) &&
           (Type::IsIntegral(to_type) || Type::IsNumeric(to_type) ||
            to_type == type::TypeId::VARCHAR ||
            to_type == type::TypeId::BOOLEAN);
  }

  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (Type::IsIntegral(to_type)) {
      // Just convert it
      return Value{to_type, codegen->CreateFPToSI(value.GetValue(), val_type)};
    } else if (to_type == type::TypeId::VARCHAR) {
      // TODO: Implement me
    }

    // TODO: Fill me out
    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

//===----------------------------------------------------------------------===//
// Timestamp casting rules
//
// We do TIMESTAMP -> {DATE, VARCHAR}
//===----------------------------------------------------------------------===//

struct CastTimestamp : public Type::Cast {
  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return from_type == type::TypeId::TIMESTAMP &&
           (to_type == type::TypeId::DATE ||
            to_type == type::TypeId::VARCHAR);
  }

  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (to_type == type::TypeId::DATE) {
      // TODO: Fix me
      llvm::Value *date = codegen->CreateSDiv(
          value.GetValue(),
          codegen.Const64(type::TimestampType::kUsecsPerDate));

      return Value{to_type, codegen->CreateTrunc(date, codegen.Int32Type())};
    } else if (to_type == type::TypeId::VARCHAR) {
      // TODO: Implement me
    }

    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

//===----------------------------------------------------------------------===//
// Date casting rules
//
// We do DATE -> {TIMESTAMP, VARCHAR}
//===----------------------------------------------------------------------===//

struct CastDate : public Type::Cast {
  bool SupportsTypes(type::TypeId from_type,
                     type::TypeId to_type) const override {
    return from_type == type::TypeId::DATE &&
           (to_type == type::TypeId::TIMESTAMP ||
            to_type == type::TypeId::VARCHAR);
  }

  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::TypeId to_type) const override {
    PL_ASSERT(SupportsTypes(value.GetType(), to_type));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (to_type == type::TypeId::TIMESTAMP) {
      // Date is number of days since 2000, timestamp is micros since same

      llvm::Value *zext_date =
          codegen->CreateZExt(value.GetValue(), codegen.Int64Type());
      llvm::Value *timestamp = codegen->CreateMul(
          zext_date, codegen.Const64(type::TimestampType::kUsecsPerDate));

      return Value{to_type, timestamp};
    } else if (to_type == type::TypeId::VARCHAR) {
      // TODO: Implement me
    }

    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

///===--------------------------------------------------------------------===///
/// COMPARISON RULES
///===--------------------------------------------------------------------===///

//===----------------------------------------------------------------------===//
// ComparisonWithNullPropagation
//
// This is a wrapper around lower-level comparisons that are not null-aware.
// This class computes the null-bit of the result of the comparison based on the
// values being compared. It delegates to the wrapped comparison function to
// perform the actual comparison. The null-bit and resulting value are combined.
//===----------------------------------------------------------------------===//

class ComparisonWithNullPropagation : public Type::Comparison {
#define _DO_WRAPPED_COMPARE(OP)                                      \
  /* Determine the null bit based on the left and right values */    \
  llvm::Value *null = left.GetNullBit();                             \
  if (null == nullptr) {                                             \
    null = right.GetNullBit();                                       \
  } else if (right.GetNullBit() != nullptr) {                        \
    null = codegen->CreateOr(null, right.GetNullBit());              \
  }                                                                  \
  /* Now perform the comparison using a non-null-aware comparison */ \
  Value result = (OP);                                               \
  /* Return the result with the computed null-bit */                 \
  return Value{result.GetType(), result.GetValue(), result.GetLength(), null};

 public:
  ComparisonWithNullPropagation(Type::Comparison &inner_comparison)
      : inner_comparison_(inner_comparison) {}

  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return inner_comparison_.SupportsTypes(left_type, right_type);
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareLt(codegen, left, right));
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareLte(codegen, left, right));
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareEq(codegen, left, right));
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareNe(codegen, left, right));
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareGt(codegen, left, right));
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    _DO_WRAPPED_COMPARE(inner_comparison_.DoCompareGte(codegen, left, right));
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    _DO_WRAPPED_COMPARE(
        inner_comparison_.DoComparisonForSort(codegen, left, right));
  }

#undef _DO_WRAPPED_COMPARE

 private:
  // The non-null-checking comparison
  Type::Comparison &inner_comparison_;
};

//===----------------------------------------------------------------------===//
// BOOL Comparison
//
// Boolean comparisons can only compare two boolean types. So the assumption for
// all the functions is that the types of both the left and right argument are
// type::TypeId::BOOLEAN.
//===----------------------------------------------------------------------===//

struct BooleanComparison : public Type::Comparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return left_type == type::TypeId::BOOLEAN && left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // For boolean sorting, we convert 1-bit boolean values into a 32-bit number
    Value casted_left = left.CastTo(codegen, type::TypeId::INTEGER);
    Value casted_right = right.CastTo(codegen, type::TypeId::INTEGER);

    return casted_left.Sub(codegen, casted_right);
  }
};

//===----------------------------------------------------------------------===//
// INTEGER Comparison
//
// Comparison functions where the left and right values are guaranteed to be one
// of the integral types (i.e., smallint, tinyint, integer, bigint).
//===----------------------------------------------------------------------===//

struct IntegerComparison : public Type::Comparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Cast appropriately
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSLT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSLE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    Value sub_result = left.Sub(codegen, right);
    return sub_result.CastTo(codegen, type::TypeId::INTEGER);
  }
};

//===----------------------------------------------------------------------===//
// DECIMAL Comparison
//
// Comparison functions where the left **and** right values are 8-byte
// floating point number (i.e., a Peloton DECIMAL SQL type)
//===----------------------------------------------------------------------===//

struct DecimalComparison : public Type::Comparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return Type::IsNumeric(left_type) && left_type == right_type;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpULT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpULE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpUEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpUNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpUGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the comparison
    llvm::Value *result =
        codegen->CreateFCmpUGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do the subtraction
    Value result = left.Sub(codegen, right);

    // Cast to 32-bit integer
    llvm::Value *casted_result =
        codegen->CreateFPToSI(result.GetValue(), codegen.Int32Type());

    // Return the result for sorting
    return Value{type::TypeId::INTEGER, casted_result};
  }
};

//===----------------------------------------------------------------------===//
// VARLEN Comparison
//
// Comparison functions where the left and right value must be variable length
// strings (or binary values)
//===----------------------------------------------------------------------===//

struct VarlenComparison : public Type::Comparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return Type::IsVariableLength(left_type) && left_type == right_type;
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
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is <= 0
    result = codegen->CreateICmpSLE(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpEQ(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpNE(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is > 0
    result = codegen->CreateICmpSGT(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is >= 0
    result = codegen->CreateICmpSGE(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Return the comparison
    return Value{type::TypeId::INTEGER, result};
  }
};

//===----------------------------------------------------------------------===//
// TIMESTAMP Comparison
//
// Comparison functions where the left and right value are timestamps
//===----------------------------------------------------------------------===//

struct TimestampComparison : public IntegerComparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return left_type == type::TypeId::TIMESTAMP &&
           left_type == right_type;
  }
};

//===----------------------------------------------------------------------===//
// DATE Comparison
//
// Comparison functions where the left and right value are dates
//===----------------------------------------------------------------------===//

struct DateComparison : public IntegerComparison {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return left_type == type::TypeId::DATE && left_type == right_type;
  }
};

///===--------------------------------------------------------------------===///
/// ARITHMETIC BINARY OPERATION RULES
///===--------------------------------------------------------------------===///

//===----------------------------------------------------------------------===//
// BinaryOperatorWithNullPropagation
//
// This is a wrapper around lower-level binary operators which are not
// null-aware. This class properly computes the result of a binary operator in
// the presence of null input values
//===----------------------------------------------------------------------===//

struct BinaryOperatorWithNullPropagation : public Type::BinaryOperator {
  BinaryOperatorWithNullPropagation(Type::BinaryOperator &inner_op)
      : inner_op_(inner_op) {}

  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return inner_op_.SupportsTypes(left_type, right_type);
  }

  type::TypeId ResultType(type::TypeId left_type,
                                type::TypeId right_type) const override {
    return inner_op_.ResultType(left_type, right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    if (!left.IsNullable() && !right.IsNullable()) {
      // Neither value is nullable, fast-path
      return inner_op_.DoWork(codegen, left, right, on_error);
    }

    // One of the inputs is nullable, compute the null bit first
    llvm::Value *null =
        codegen->CreateOr(left.IsNull(codegen), right.IsNull(codegen));

    Value null_val, ret_val;
    If is_null{codegen, null};
    {
      // If either value is null, the result of the operator is null
      null_val = Type::GetNullValue(
          codegen, ResultType(left.GetType(), right.GetType()));
    }
    is_null.ElseBlock();
    {
      // If both values are not null, perform the non-null-aware operation
      ret_val = inner_op_.DoWork(codegen, left, right, on_error);
    }
    is_null.EndIf();

    return is_null.BuildPHI(null_val, ret_val);
  }

 private:
  // The non-null-aware operator
  Type::BinaryOperator &inner_op_;
};

//===----------------------------------------------------------------------===//
// INTEGER MATH OPS
//
// All arithmetic operations on integral values assume:
// 1. Both input values are one of the Peloton integer types (i.e., smallint,
//    tinyint, integer, or bigint).
// 2. Both input values have the same SQL type. This means that all (implicit
//    or explicit casting) is done **outside** these functions.
// 3. Both input values are not NULL.
//===----------------------------------------------------------------------===//

struct IntegerOps : public Type::BinaryOperator {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && left_type == right_type;
  }

  type::TypeId ResultType(type::TypeId left_type,
                                type::TypeId right_type) const override {
    return static_cast<type::TypeId>(std::max(
        static_cast<uint32_t>(left_type), static_cast<uint32_t>(right_type)));
  }
};

struct IntegerAdd : public IntegerOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallAddWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerSub : public IntegerOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do subtraction
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerMul : public IntegerOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do multiplication
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallMulWithOverflow(
        left.GetValue(), right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerDiv : public IntegerOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    codegen::Value div0 = right.CompareEq(
        codegen, Value{type::TypeId::INTEGER, codegen.Const32(0)});

    // Check if the caller cares about division-by-zero errors

    llvm::Value *result = nullptr;
    if (on_error == Value::OnError::ReturnDefault ||
        on_error == Value::OnError::ReturnNull) {
      // Get the default value for the types
      llvm::Value *default_val = nullptr, *division_result = nullptr;
      If is_div0{codegen, div0.GetValue()};
      {
        if (on_error == Value::OnError::ReturnDefault) {
          default_val =
              Type::GetDefaultValue(codegen, left.GetType()).GetValue();
        } else {
          default_val = Type::GetNullValue(codegen, left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result =
            codegen->CreateSDiv(left.GetValue(), right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do division
      result = codegen->CreateSDiv(left.GetValue(), right.GetValue());
    }

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerMod : public IntegerOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    codegen::Value div0 = right.CompareEq(
        codegen, Value{type::TypeId::INTEGER, codegen.Const32(0)});

    // Check if the caller cares about division-by-zero errors

    llvm::Value *result = nullptr;
    if (on_error == Value::OnError::ReturnDefault ||
        on_error == Value::OnError::ReturnNull) {
      // Get the default value for the types
      llvm::Value *default_val = nullptr, *division_result = nullptr;
      If is_div0{codegen, div0.GetValue()};
      {
        if (on_error == Value::OnError::ReturnDefault) {
          default_val =
              Type::GetDefaultValue(codegen, left.GetType()).GetValue();
        } else {
          default_val = Type::GetNullValue(codegen, left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result =
            codegen->CreateSRem(left.GetValue(), right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do division
      result = codegen->CreateSRem(left.GetValue(), right.GetValue());
    }

    // Return result
    return Value{left.GetType(), result};
  }
};

//===----------------------------------------------------------------------===//
// DECIMAL MATH OPS
//
// All arithmetic operations on decimal/numeric values assume:
// 1. Both input values are one of the Peloton decimal/numeric types
//    (i.e., decimal).
// 2. Both input values have the same SQL type. This means that all (implicit
//    or explicit casting) is done **outside** these functions.
// 3. Both input values are not NULL.
//===----------------------------------------------------------------------===//

struct DecimalOps : public Type::BinaryOperator {
  bool SupportsTypes(type::TypeId left_type,
                     type::TypeId right_type) const override {
    return Type::IsNumeric(left_type) && left_type == right_type;
  }

  type::TypeId ResultType(
      UNUSED_ATTRIBUTE type::TypeId left_type,
      UNUSED_ATTRIBUTE type::TypeId right_type) const override {
    return type::TypeId::DECIMAL;
  }
};

struct DecimalAdd : public DecimalOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *result =
        codegen->CreateFAdd(left.GetValue(), right.GetValue());

    // Return result
    return Value{type::TypeId::DECIMAL, result};
  }
};

struct DecimalSub : public DecimalOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *result =
        codegen->CreateFSub(left.GetValue(), right.GetValue());

    // Return result
    return Value{type::TypeId::DECIMAL, result};
  }
};

struct DecimalMul : public DecimalOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // Do addition
    llvm::Value *result =
        codegen->CreateFMul(left.GetValue(), right.GetValue());

    // Return result
    return Value{type::TypeId::DECIMAL, result};
  }
};

struct DecimalDiv : public DecimalOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    codegen::Value zero{type::TypeId::DECIMAL, codegen.ConstDouble(0.0)};
    codegen::Value div0 = right.CompareEq(codegen, zero);

    llvm::Value *result = nullptr;
    if (on_error == Value::OnError::ReturnDefault ||
        on_error == Value::OnError::ReturnNull) {
      // Get the default value for the types
      llvm::Value *default_val = nullptr, *division_result = nullptr;
      If is_div0{codegen, div0.GetValue()};
      {
        if (on_error == Value::OnError::ReturnDefault) {
          default_val =
              Type::GetDefaultValue(codegen, left.GetType()).GetValue();
        } else {
          default_val = Type::GetNullValue(codegen, left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result =
            codegen->CreateFDiv(left.GetValue(), right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do division
      result = codegen->CreateFDiv(left.GetValue(), right.GetValue());
    }

    // Return result
    return Value{type::TypeId::DECIMAL, result};
  }
};

struct DecimalMod : public DecimalOps {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               UNUSED_ATTRIBUTE Value::OnError on_error) const override {
    PL_ASSERT(SupportsTypes(left.GetType(), right.GetType()));

    // First, check if the divisor is zero
    codegen::Value zero{type::TypeId::DECIMAL, codegen.ConstDouble(0.0)};
    codegen::Value div0 = right.CompareEq(codegen, zero);

    llvm::Value *result = nullptr;
    if (on_error == Value::OnError::ReturnDefault ||
        on_error == Value::OnError::ReturnNull) {
      // Get the default value for the types
      llvm::Value *default_val = nullptr, *division_result = nullptr;
      If is_div0{codegen, div0.GetValue()};
      {
        if (on_error == Value::OnError::ReturnDefault) {
          default_val =
              Type::GetDefaultValue(codegen, left.GetType()).GetValue();
        } else {
          default_val = Type::GetNullValue(codegen, left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result =
            codegen->CreateFRem(left.GetValue(), right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do modulo
      result = codegen->CreateFRem(left.GetValue(), right.GetValue());
    }

    // Return result
    return Value{type::TypeId::DECIMAL, result};
  }
};

//===----------------------------------------------------------------------===//
// The static comparison and binary functions
//===----------------------------------------------------------------------===//

static CastBoolean kCastBoolean;
static CastInteger kCastInteger;
static CastDecimal kCastDecimal;
static CastTimestamp kCastTimestamp;
static CastDate kCastDate;

static CastWithNullPropagation kWrappedCastBoolean{kCastBoolean};
static CastWithNullPropagation kWrappedCastInteger{kCastInteger};
static CastWithNullPropagation kWrappedCastDecimal{kCastDecimal};
static CastWithNullPropagation kWrappedCastTimestamp{kCastTimestamp};
static CastWithNullPropagation kWrappedCastDate{kCastDate};

static BooleanComparison kBooleanComparison;
static IntegerComparison kIntegerComparison;
static DecimalComparison kDecimalComparison;
static VarlenComparison kVarlenComparison;
static TimestampComparison kTimestampComparison;
static DateComparison kDateComparison;

static ComparisonWithNullPropagation kWrappedBooleanComparison{
    kBooleanComparison};
static ComparisonWithNullPropagation kWrappedIntegerComparison{
    kIntegerComparison};
static ComparisonWithNullPropagation kWrappedDecimalComparison{
    kDecimalComparison};
static ComparisonWithNullPropagation kWrappedVarlenComparison{
    kVarlenComparison};
static ComparisonWithNullPropagation kWrappedTimestampComparison{
    kTimestampComparison};
static ComparisonWithNullPropagation kWrappedDateComparison{kDateComparison};

static IntegerAdd kIntegerAdd;
static IntegerSub kIntegerSub;
static IntegerMul kIntegerMul;
static IntegerDiv kIntegerDiv;
static IntegerMod kIntegerMod;

static BinaryOperatorWithNullPropagation kWrappedIntegerAdd{kIntegerAdd};
static BinaryOperatorWithNullPropagation kWrappedIntegerSub{kIntegerSub};
static BinaryOperatorWithNullPropagation kWrappedIntegerMul{kIntegerMul};
static BinaryOperatorWithNullPropagation kWrappedIntegerDiv{kIntegerDiv};
static BinaryOperatorWithNullPropagation kWrappedIntegerMod{kIntegerMod};

static DecimalAdd kDecimalAdd;
static DecimalSub kDecimalSub;
static DecimalMul kDecimalMul;
static DecimalDiv kDecimalDiv;
static DecimalMod kDecimalMod;

static BinaryOperatorWithNullPropagation kWrappedDecimalAdd{kDecimalAdd};
static BinaryOperatorWithNullPropagation kWrappedDecimalSub{kDecimalSub};
static BinaryOperatorWithNullPropagation kWrappedDecimalMul{kDecimalMul};
static BinaryOperatorWithNullPropagation kWrappedDecimalDiv{kDecimalDiv};
static BinaryOperatorWithNullPropagation kWrappedDecimalMod{kDecimalMod};

// String representation of all binary operators
const std::string Type::kOpNames[] = {"Negation", "Abs", "Add", "Sub",
                                      "Mul",      "Div", "Mod"};

Type::ImplicitCastTable Type::kImplicitCastsTable = {
    // INVALID Casts ...
    {type::TypeId::INVALID, {}},
    {type::TypeId::PARAMETER_OFFSET, {}},

    // Boolean's can only be implicitly casted to integers
    {type::TypeId::BOOLEAN, {type::TypeId::BOOLEAN}},

    // Tinyint's can be implicitly casted to any of the integral types
    {type::TypeId::TINYINT,
     {type::TypeId::TINYINT, type::TypeId::SMALLINT,
      type::TypeId::INTEGER, type::TypeId::BIGINT,
      type::TypeId::DECIMAL}},

    // Smallints's can be implicitly casted to any of the integral types
    {type::TypeId::SMALLINT,
     {type::TypeId::SMALLINT, type::TypeId::INTEGER,
      type::TypeId::BIGINT, type::TypeId::DECIMAL}},

    // Integers's can be implicitly casted to any of the integral types
    {type::TypeId::INTEGER,
     {type::TypeId::INTEGER, type::TypeId::BIGINT,
      type::TypeId::DECIMAL}},

    // Tinyint's can be implicitly casted to any of the integral types
    {type::TypeId::BIGINT,
     {type::TypeId::BIGINT, type::TypeId::DECIMAL}},

    // Decimal's can be implicitly casted to any of the integral types
    {type::TypeId::DECIMAL, {type::TypeId::DECIMAL}},

    // Timestamp's can only be implicitly casted to DATE
    {type::TypeId::TIMESTAMP,
     {type::TypeId::TIMESTAMP, type::TypeId::DATE}},

    // Date's can only be implicitly casted to TIMESTAMP
    {type::TypeId::DATE,
     {type::TypeId::DATE, type::TypeId::TIMESTAMP}},

    // Varchars's can only be implicitly casted to itself
    {type::TypeId::VARCHAR, {type::TypeId::VARCHAR}},

    // VARBINARY's can only be implicitly casted to itself
    {type::TypeId::VARBINARY, {type::TypeId::VARBINARY}},

    // ARRAY's can only be implicitly casted to itself
    {type::TypeId::VARBINARY, {type::TypeId::ARRAY}},

    // UDT's define their own casting
    {type::TypeId::UDT, {}},
};

Type::CastingTable Type::kCastingTable = {
    {type::TypeId::INVALID, {}},
    {type::TypeId::PARAMETER_OFFSET, {}},
    {type::TypeId::BOOLEAN, {&kWrappedCastBoolean}},
    {type::TypeId::TINYINT, {&kWrappedCastInteger}},
    {type::TypeId::SMALLINT, {&kWrappedCastInteger}},
    {type::TypeId::INTEGER, {&kWrappedCastInteger}},
    {type::TypeId::BIGINT, {&kWrappedCastInteger}},
    {type::TypeId::DECIMAL, {&kWrappedCastDecimal}},
    {type::TypeId::TIMESTAMP, {&kWrappedCastTimestamp}},
    {type::TypeId::DATE, {&kWrappedCastDate}},
    {type::TypeId::VARCHAR, {}},
    {type::TypeId::VARBINARY, {}},
    {type::TypeId::ARRAY, {}},
    {type::TypeId::UDT, {}}};

Type::ComparisonTable Type::kComparisonTable = {
    {type::TypeId::INVALID, {}},
    {type::TypeId::PARAMETER_OFFSET, {}},
    {type::TypeId::BOOLEAN, {&kWrappedBooleanComparison}},
    {type::TypeId::TINYINT, {&kWrappedIntegerComparison}},
    {type::TypeId::SMALLINT, {&kWrappedIntegerComparison}},
    {type::TypeId::INTEGER, {&kWrappedIntegerComparison}},
    {type::TypeId::BIGINT, {&kWrappedIntegerComparison}},
    {type::TypeId::DECIMAL, {&kWrappedDecimalComparison}},
    {type::TypeId::TIMESTAMP, {&kWrappedTimestampComparison}},
    {type::TypeId::DATE, {&kWrappedDateComparison}},
    {type::TypeId::VARCHAR, {&kWrappedVarlenComparison}},
    {type::TypeId::VARBINARY, {}},
    {type::TypeId::ARRAY, {}},
    {type::TypeId::UDT, {}}};

Type::BinaryOperatorTable Type::kBuiltinBinaryOperatorsTable = {
    {Type::OperatorId::Add, {&kWrappedIntegerAdd, &kWrappedDecimalAdd}},
    {Type::OperatorId::Sub, {&kWrappedIntegerSub, &kWrappedDecimalSub}},
    {Type::OperatorId::Mul, {&kWrappedIntegerMul, &kWrappedDecimalMul}},
    {Type::OperatorId::Div, {&kWrappedIntegerDiv, &kWrappedDecimalDiv}},
    {Type::OperatorId::Mod, {&kWrappedIntegerMod, &kWrappedDecimalMod}}};

// Get the number of bytes needed to store the given type
uint32_t Type::GetFixedSizeForType(type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
    case type::TypeId::TINYINT:
      return 1;
    case type::TypeId::SMALLINT:
      return 2;
    case type::TypeId::INTEGER:
    case type::TypeId::DATE:
      return 4;
    case type::TypeId::BIGINT:
    case type::TypeId::DECIMAL:
    case type::TypeId::TIMESTAMP:
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
    case type::TypeId::ARRAY:
      return 8;
    default:
      break;
  }
  std::string msg = StringUtil::Format("Type '%s' doesn't have a fixed size",
                                       TypeIdToString(type_id).c_str());
  throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
}

bool Type::IsIntegral(type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::TINYINT:
    case type::TypeId::SMALLINT:
    case type::TypeId::INTEGER:
    case type::TypeId::BIGINT:
      return true;
    default:
      return false;
  }
}

void Type::GetTypeForMaterialization(CodeGen &codegen,
                                     type::TypeId type_id,
                                     llvm::Type *&val_type,
                                     llvm::Type *&len_type) {
  PL_ASSERT(type_id != type::TypeId::INVALID);
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      val_type = codegen.BoolType();
      break;
    case type::TypeId::TINYINT:
      val_type = codegen.Int8Type();
      break;
    case type::TypeId::SMALLINT:
      val_type = codegen.Int16Type();
      break;
    case type::TypeId::DATE:
    case type::TypeId::INTEGER:
      val_type = codegen.Int32Type();
      break;
    case type::TypeId::TIMESTAMP:
    case type::TypeId::BIGINT:
      val_type = codegen.Int64Type();
      break;
    case type::TypeId::DECIMAL:
      val_type = codegen.DoubleType();
      break;
    case type::TypeId::VARBINARY:
    case type::TypeId::VARCHAR:
      val_type = codegen.CharPtrType();
      len_type = codegen.Int32Type();
      break;
    default: {
      throw Exception{StringUtil::Format("'%s' is not a materializable type",
                                         TypeIdToString(type_id).c_str())};
    }
  }
}

bool Type::CanImplicitlyCastTo(type::TypeId from_type,
                               type::TypeId to_type) {
  const auto &implicit_casts = kImplicitCastsTable[from_type];
  for (auto &castable_type : implicit_casts) {
    if (castable_type == to_type) {
      return true;
    }
  }
  return false;
}

Value Type::GetMinValue(CodeGen &codegen, type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_MIN)};
    case type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_MIN)};
    case type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_MIN)};
    case type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_MIN)};
    case type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_MIN)};
    case type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_MIN)};
    case type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_MIN)};
    case type::TypeId::DATE:
      return Value{type_id, codegen.Const32(type::PELOTON_DATE_MIN)};
    default: {
      auto msg = StringUtil::Format("No minimum value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetMaxValue(CodeGen &codegen, type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_MAX)};
    case type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_MAX)};
    case type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_MAX)};
    case type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_MAX)};
    case type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_MAX)};
    case type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_MAX)};
    case type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_MAX)};
    case type::TypeId::DATE:
      return Value{type_id, codegen.Const64(type::PELOTON_DATE_MAX)};
    default: {
      auto msg = StringUtil::Format("No maximum value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetNullValue(CodeGen &codegen, type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_NULL),
                   nullptr, codegen.ConstBool(true)};
    case type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_NULL), nullptr,
                   codegen.ConstBool(true)};
    case type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_NULL), nullptr,
                   codegen.ConstBool(true)};
    case type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_NULL), nullptr,
                   codegen.ConstBool(true)};
    case type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_NULL), nullptr,
                   codegen.ConstBool(true)};
    case type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_NULL),
                   nullptr, codegen.ConstBool(true)};
    case type::TypeId::DATE:
      return Value{type_id, codegen.Const32(type::PELOTON_DATE_NULL), nullptr,
                   codegen.ConstBool(true)};
    case type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_NULL),
                   nullptr, codegen.ConstBool(true)};
    case type::TypeId::VARBINARY:
    case type::TypeId::VARCHAR:
      return Value{type_id, codegen.NullPtr(codegen.CharPtrType()),
                   codegen.Const32(0), codegen.ConstBool(true)};
    default: {
      auto msg = StringUtil::Format("No null value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetDefaultValue(CodeGen &codegen, type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(false)};
    case type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(0)};
    case type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(0)};
    case type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(0)};
    case type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(0)};
    case type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(0.0)};
    case type::TypeId::DATE:
      return Value{type_id, codegen.Const32(0)};
    case type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(0)};
    default: {
      auto msg = StringUtil::Format("No default value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

const Type::Cast *Type::GetCast(type::TypeId from_type,
                                type::TypeId to_type) {
  const auto &casting_impls = kCastingTable[from_type];
  for (const auto &cast_impl : casting_impls) {
    if (cast_impl->SupportsTypes(from_type, to_type)) {
      return cast_impl;
    }
  }

  // Error
  throw CastException{from_type, to_type};
}

const Type::Comparison *Type::GetComparison(
    type::TypeId left_type, type::TypeId &left_casted_type,
    type::TypeId right_type, type::TypeId &right_casted_type) {
  // Operator resolution works as follows:
  // 1. Try to find an implementation that requires no implicit casting.
  // 2. Try to find an implementation that requires casting only the left input
  // 3. Try to find an implementation that requires casting only the right input
  //
  // NOTE: Step 1 is rolled into the Step 2 because a type is (trivially)
  //       implicitly cast-able to itself, and appears first in the implicit
  //       casting table.

  for (auto &implicit_casted_left : kImplicitCastsTable[left_type]) {
    for (const auto *cmp_func : kComparisonTable[implicit_casted_left]) {
      if (cmp_func->SupportsTypes(implicit_casted_left, right_type)) {
        left_casted_type = implicit_casted_left;
        return cmp_func;
      }
    }
  }

  for (auto &implicit_casted_right : kImplicitCastsTable[right_type]) {
    for (const auto *cmp_func : kComparisonTable[implicit_casted_right]) {
      if (cmp_func->SupportsTypes(left_type, implicit_casted_right)) {
        right_casted_type = implicit_casted_right;
        return cmp_func;
      }
    }
  }

  // Error
  std::string msg = StringUtil::Format(
      "No comparison rule between types %s and %s",
      TypeIdToString(left_type).c_str(), TypeIdToString(right_type).c_str());
  throw Exception{msg};
}

const Type::BinaryOperator *Type::GetBinaryOperator(
    OperatorId op_id, type::TypeId left_type,
    type::TypeId &left_casted_type, type::TypeId right_type,
    type::TypeId &right_casted_type) {
  const auto &iter = kBuiltinBinaryOperatorsTable.find(op_id);
  PL_ASSERT(iter != kBuiltinBinaryOperatorsTable.end());

  // The list of candidate operator implementations
  const auto &candidates = iter->second;

  // Operator resolution works as follows:
  // 1. Try to find an implementation that requires no implicit casting.
  // 2. Try to find an implementation that requires casting only the left input
  // 3. Try to find an implementation that requires casting only the right input
  //
  // NOTE: Step 1 is rolled into the Step 2 because a type is (trivially)
  //       implicitly cast-able to itself, and appears first in the implicit
  //       casting table.

  for (auto &implicit_casted_left : kImplicitCastsTable[left_type]) {
    for (const auto *op_func : candidates) {
      if (op_func->SupportsTypes(implicit_casted_left, right_type)) {
        left_casted_type = implicit_casted_left;
        return op_func;
      }
    }
  }

  for (auto &implicit_casted_right : kImplicitCastsTable[right_type]) {
    for (const auto *op_func : candidates) {
      if (op_func->SupportsTypes(left_type, implicit_casted_right)) {
        right_casted_type = implicit_casted_right;
        return op_func;
      }
    }
  }

  // Error
  std::string msg = StringUtil::Format(
      "No compatible '%s' operator for input types: %s, %s",
      kOpNames[static_cast<uint32_t>(op_id)].c_str(),
      TypeIdToString(left_type).c_str(), TypeIdToString(right_type).c_str());
  throw Exception{msg};
}

}  // namespace codegen
}  // namespace peloton
