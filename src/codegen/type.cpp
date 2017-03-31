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

#include "codegen/values_runtime_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Default Comparison
//===----------------------------------------------------------------------===//

// Main comparison functions
Value Type::Comparison::DoCompareLt(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  throw Exception{"Invalid LT comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoCompareLte(UNUSED_ATTRIBUTE CodeGen &,
                                     const Value &left,
                                     const Value &right) const {
  throw Exception{"Invalid LE comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoCompareEq(UNUSED_ATTRIBUTE CodeGen &,
                                    const Value &left,
                                    const Value &right) const {
  throw Exception{"Invalid EQ comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoCompareNe(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  throw Exception{"Invalid NE comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoCompareGt(UNUSED_ATTRIBUTE CodeGen &,
                                    const Value &left,
                                    const Value &right) const {
  throw Exception{"Invalid GT comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoCompareGte(UNUSED_ATTRIBUTE CodeGen &,
                                     const Value &left,
                                     const Value &right) const {
  throw Exception{"Invalid GE comparison of " + TypeIdToString(left.GetType()) +
                  " type with " + TypeIdToString(right.GetType())};
}

Value Type::Comparison::DoComparisonForSort(UNUSED_ATTRIBUTE CodeGen &,
                                            const Value &left,
                                            const Value &right) const {
  throw Exception{"Invalid SORT comparison of " +
                  TypeIdToString(left.GetType()) + " type with " +
                  TypeIdToString(right.GetType())};
}

//===----------------------------------------------------------------------===//
// BOOL Comparison
//
// Boolean comparisons can only compare two boolean types. So the assumption for
// all the functions is that the types of both the left and right argument are
// type::Type::TypeId::BOOLEAN.
//===----------------------------------------------------------------------===//

struct BooleanComparison : public Type::Comparison {
  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpULE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpNE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGT(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // Do the comparison
    llvm::Value *result =
        codegen->CreateICmpUGE(left.GetValue(), right.GetValue());

    // Return the result
    return Value{left.GetType(), result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::BOOLEAN);

    // For boolean sorting, we convert 1-bit boolean values into a 32-bit number
    llvm::Type *int32 = codegen.Int32Type();

    llvm::Value *casted_left = codegen->CreateZExt(left.GetValue(), int32);
    llvm::Value *casted_right = codegen->CreateZExt(right.GetValue(), int32);

    // Just subtract the values to get a stable sort
    llvm::Value *result = codegen->CreateSub(casted_left, casted_right);

    // Return the result of the comparison
    return Value{type::Type::TypeId::INTEGER, result};
  }
};

//===----------------------------------------------------------------------===//
// INTEGER Comparison
//
// Comparison functions where the left value is guaranteed to be an integral
// type of arbitrary bit size.
//===----------------------------------------------------------------------===//

struct IntegerComparison : public Type::Comparison {
  llvm::Value *CastToCorrectType(CodeGen &codegen, const Value &o,
                                 llvm::Type *llvm_type) const {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right = nullptr;
    if (o.GetType() == type::Type::TypeId::DECIMAL) {
      // Cast the right-side values from a floating point value to integer
      raw_right = codegen->CreateFPToSI(o.GetValue(), llvm_type);
    } else {
      raw_right = codegen->CreateSExtOrTrunc(o.GetValue(), llvm_type);
    }
    return raw_right;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpSLT(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpSLE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpEQ(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpNE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpSGT(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateICmpSGE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the subtraction
    llvm::Value *result = codegen->CreateSub(left.GetValue(), raw_right);

    // Cast to 32-bit integer
    llvm::Type *int32 = codegen.Int32Type();
    llvm::Value *casted_result = codegen->CreateZExtOrBitCast(result, int32);

    // Return the result
    return Value{type::Type::TypeId::INTEGER, casted_result};
  }
};

//===----------------------------------------------------------------------===//
// DECIMAL Comparison
//
// Comparison functions where the left value is guaranteed to be an 8-byte
// floating point number (i.e., a Peloton DECIMAL SQL type)
//===----------------------------------------------------------------------===//

struct DecimalComparison : public Type::Comparison {
  llvm::Value *CastToCorrectType(CodeGen &codegen, const Value &o,
                                 llvm::Type *llvm_type) const {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right = o.GetValue();
    if (o.GetType() != type::Type::TypeId::DECIMAL) {
      // Cast the right-side values from a floating point value to integer
      raw_right = codegen->CreateSIToFP(o.GetValue(), llvm_type);
    }
    return raw_right;
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpULT(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpULE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpUEQ(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpUNE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpUGT(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the comparison
    llvm::Value *result = codegen->CreateFCmpUGE(left.GetValue(), raw_right);

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // Properly cast the right value to the correct type
    llvm::Value *raw_right =
        CastToCorrectType(codegen, right, left.GetValue()->getType());

    // Do the subtraction
    llvm::Value *result = codegen->CreateFSub(left.GetValue(), raw_right);

    // Cast to 32-bit integer
    llvm::Type *int32 = codegen.Int32Type();
    llvm::Value *casted_result = codegen->CreateFPToSI(result, int32);

    // Return the result for sorting
    return Value{type::Type::TypeId::INTEGER, casted_result};
  }
};

//===----------------------------------------------------------------------===//
// VARLEN Comparison
//
// Comparison functions where the left value is guaranteed to be variable length
// string or binary value
//===----------------------------------------------------------------------===//

struct VarlenComparison : public Type::Comparison {
  llvm::Value *CallCompareStrings(CodeGen &codegen, const Value &left,
                                  const Value &right) const {
    // Get the proxy to ValuesRuntime::CompareStrings(...)
    auto *cmp_func = ValuesRuntimeProxy::_CompareStrings::GetFunction(codegen);

    // Setup the function arguments to the call
    auto args = {left.GetValue(), left.GetLength(), right.GetValue(),
                 right.GetLength()};

    // Make the function call
    return codegen.CallFunc(cmp_func, args);
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is < 0
    result = codegen->CreateICmpSLT(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is <= 0
    result = codegen->CreateICmpSLE(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpEQ(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is == 0
    result = codegen->CreateICmpNE(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is > 0
    result = codegen->CreateICmpSGT(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Check if the result is >= 0
    result = codegen->CreateICmpSGT(result, codegen.Const32(0));

    // Return the comparison
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    PL_ASSERT(right.GetType() == type::Type::TypeId::VARCHAR);

    // Call CompareStrings(...)
    llvm::Value *result = CallCompareStrings(codegen, left, right);

    // Return the comparison
    return Value{type::Type::TypeId::INTEGER, result};
  }
};

//===----------------------------------------------------------------------===//
// INTEGER OPS
//
// This class assumes both the left and right inputs and integer values of the
// same type
//===----------------------------------------------------------------------===//

struct IntegerAdd : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result = codegen->CreateAdd(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerSub : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result = codegen->CreateSub(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerMul : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result = codegen->CreateMul(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerDiv : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateSDiv(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct IntegerMod : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateSRem(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

//===----------------------------------------------------------------------===//
// MIXED INTEGER OPS
//
// This class assumes both the left and right inputs and integer values of
// different types.
//===----------------------------------------------------------------------===//

struct MixedIntegerAdd : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    llvm::Type *left_type = left.GetValue()->getType();
    llvm::Type *right_type = right.GetValue()->getType();

    llvm::Value *left_value = nullptr, *right_value = nullptr;
    type::Type::TypeId result_type;

    if (left_type->getIntegerBitWidth() > right_type->getIntegerBitWidth()) {
      // left size is larger than right => cast right upwards and perform op
      left_value = left.GetValue();
      right_value = codegen->CreateZExt(right.GetValue(), left_type);
      result_type = left.GetType();
    } else {
      // right size is larger than right => cast left upwards and perform op
      left_value = codegen->CreateZExt(left.GetValue(), right_type);
      right_value = right.GetValue();
      result_type = right.GetType();
    }

    // Do op
    llvm::Value *result = codegen->CreateAdd(left_value, right_value);

    // Return result
    return Value{result_type, result};
  }
};

struct MixedIntegerSub : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    llvm::Type *left_type = left.GetValue()->getType();
    llvm::Type *right_type = right.GetValue()->getType();

    llvm::Value *left_value = nullptr, *right_value = nullptr;
    type::Type::TypeId result_type;

    if (left_type->getIntegerBitWidth() > right_type->getIntegerBitWidth()) {
      // left size is larger than right => cast right upwards and perform op
      left_value = left.GetValue();
      right_value = codegen->CreateZExt(right.GetValue(), left_type);
      result_type = left.GetType();
    } else {
      // right size is larger than right => cast left upwards and perform op
      left_value = codegen->CreateZExt(left.GetValue(), right_type);
      right_value = right.GetValue();
      result_type = right.GetType();
    }

    // Do op
    llvm::Value *result = codegen->CreateSub(left_value, right_value);

    // Return result
    return Value{result_type, result};
  }
};

struct MixedIntegerMul : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    llvm::Type *left_type = left.GetValue()->getType();
    llvm::Type *right_type = right.GetValue()->getType();

    llvm::Value *left_value = nullptr, *right_value = nullptr;
    type::Type::TypeId result_type;

    if (left_type->getIntegerBitWidth() > right_type->getIntegerBitWidth()) {
      // left size is larger than right => cast right upwards and perform op
      left_value = left.GetValue();
      right_value = codegen->CreateZExt(right.GetValue(), left_type);
      result_type = left.GetType();
    } else {
      // right size is larger than right => cast left upwards and perform op
      left_value = codegen->CreateZExt(left.GetValue(), right_type);
      right_value = right.GetValue();
      result_type = right.GetType();
    }

    // Do op
    llvm::Value *result = codegen->CreateMul(left_value, right_value);

    // Return result
    return Value{result_type, result};
  }
};

struct MixedIntegerDiv : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    llvm::Type *left_type = left.GetValue()->getType();
    llvm::Type *right_type = right.GetValue()->getType();

    llvm::Value *left_value = nullptr, *right_value = nullptr;
    type::Type::TypeId result_type;

    if (left_type->getIntegerBitWidth() > right_type->getIntegerBitWidth()) {
      // left size is larger than right => cast right upwards and perform op
      left_value = left.GetValue();
      right_value = codegen->CreateZExt(right.GetValue(), left_type);
      result_type = left.GetType();
    } else {
      // right size is larger than right => cast left upwards and perform op
      left_value = codegen->CreateZExt(left.GetValue(), right_type);
      right_value = right.GetValue();
      result_type = right.GetType();
    }

    // Do op
    llvm::Value *result = codegen->CreateSDiv(left_value, right_value);

    // Return result
    return Value{result_type, result};
  }
};

struct MixedIntegerMod : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    llvm::Type *left_type = left.GetValue()->getType();
    llvm::Type *right_type = right.GetValue()->getType();

    llvm::Value *left_value = nullptr, *right_value = nullptr;
    type::Type::TypeId result_type;

    if (left_type->getIntegerBitWidth() > right_type->getIntegerBitWidth()) {
      // left size is larger than right => cast right upwards and perform op
      left_value = left.GetValue();
      right_value = codegen->CreateZExt(right.GetValue(), left_type);
      result_type = left.GetType();
    } else {
      // right size is larger than right => cast left upwards and perform op
      left_value = codegen->CreateZExt(left.GetValue(), right_type);
      right_value = right.GetValue();
      result_type = right.GetType();
    }

    // Do op
    llvm::Value *result = codegen->CreateSRem(left_value, right_value);

    // Return result
    return Value{result_type, result};
  }
};

//===----------------------------------------------------------------------===//
// DECIMAL OPS
//
// This class assumes the left value is a decimal value
//===----------------------------------------------------------------------===//

struct DecimalAdd : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFAdd(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalSub : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFSub(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalMul : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFMul(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalDiv : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFDiv(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalMod : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left,
               const Value &right) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFRem(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

//===----------------------------------------------------------------------===//
// The static comparison and binary functions
//===----------------------------------------------------------------------===//

static Type::Comparison kComparison;
static BooleanComparison kBooleanComparison;
static IntegerComparison kIntegerComparison;
static DecimalComparison kDecimalComparison;
static VarlenComparison kVarlenComparison;

static IntegerAdd kIntegerAdd;
static IntegerSub kIntegerSub;
static IntegerMul kIntegerMul;
static IntegerDiv kIntegerDiv;
static IntegerMod kIntegerMod;

static MixedIntegerAdd kMixedIntegerAdd;
static MixedIntegerSub kMixedIntegerSub;
static MixedIntegerMul kMixedIntegerMul;
static MixedIntegerDiv kMixedIntegerDiv;
static MixedIntegerMod kMixedIntegerMod;

static DecimalAdd kDecimalAdd;
static DecimalSub kDecimalSub;
static DecimalMul kDecimalMul;
static DecimalDiv kDecimalDiv;
static DecimalMod kDecimalMod;

// String representation of all binary operators
const std::string Type::kBinaryOpNames[] = {"Add", "Sub", "Mul", "Div", "Mod"};

std::vector<const Type::Comparison *> Type::kComparisonTable = {
    &kComparison,         // Invalid
    &kIntegerComparison,  // Param offset
    &kBooleanComparison,  // Boolean
    &kIntegerComparison,  // Tiny Int
    &kIntegerComparison,  // Small Int
    &kIntegerComparison,  // Integer
    &kIntegerComparison,  // BigInt
    &kDecimalComparison,  // Decimal
    &kIntegerComparison,  // Timestamp
    &kIntegerComparison,  // Date
    &kVarlenComparison,   // Varchar
    &kVarlenComparison,   // Varbinary
    &kComparison,         // Array
    &kComparison};        // UDT

Type::BinaryOperatorTable Type::kBuiltinBinaryOperatorsTable = {
    {Type::BinaryOperatorId::Add,
     {&kIntegerAdd, &kDecimalAdd, &kMixedIntegerAdd}},

    {Type::BinaryOperatorId::Sub,
     {&kIntegerSub, &kDecimalSub, &kMixedIntegerSub}},

    {Type::BinaryOperatorId::Mul,
     {&kIntegerMul, &kDecimalMul, &kMixedIntegerMul}},

    {Type::BinaryOperatorId::Div,
     {&kIntegerDiv, &kDecimalDiv, &kMixedIntegerDiv}},

    {Type::BinaryOperatorId::Mod,
     {&kIntegerMod, &kDecimalMod, &kMixedIntegerMod}}};

bool Type::HasVariableLength(type::Type::TypeId type_id) {
  return type_id == type::Type::TypeId::VARCHAR ||
         type_id == type::Type::TypeId::VARBINARY;
}

bool Type::IsIntegral(type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::TINYINT:
    case type::Type::TypeId::SMALLINT:
    case type::Type::TypeId::INTEGER:
    case type::Type::TypeId::BIGINT:
      return true;
    default:
      return false;
  }
}

void Type::GetTypeForMaterialization(CodeGen &codegen,
                                     type::Type::TypeId type_id,
                                     llvm::Type *&val_type,
                                     llvm::Type *&len_type) {
  PL_ASSERT(type_id != type::Type::TypeId::INVALID);
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      val_type = codegen.BoolType();
      break;
    case type::Type::TypeId::TINYINT:
      val_type = codegen.Int8Type();
      break;
    case type::Type::TypeId::SMALLINT:
      val_type = codegen.Int16Type();
      break;
    case type::Type::TypeId::DATE:
    case type::Type::TypeId::INTEGER:
      val_type = codegen.Int32Type();
      break;
    case type::Type::TypeId::TIMESTAMP:
    case type::Type::TypeId::BIGINT:
      val_type = codegen.Int64Type();
      break;
    case type::Type::TypeId::DECIMAL:
      val_type = codegen.DoubleType();
      break;
    case type::Type::TypeId::VARBINARY:
    case type::Type::TypeId::VARCHAR:
      val_type = codegen.CharPtrType();
      len_type = codegen.Int32Type();
      break;
    default: {
      throw Exception{TypeIdToString(type_id) + " not materializable type"};
    }
  }
}

llvm::Value *Type::GetNullValue(CodeGen &codegen, type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      return codegen.ConstBool(type::PELOTON_BOOLEAN_NULL);
    case type::Type::TypeId::TINYINT:
      return codegen.Const8(type::PELOTON_INT8_NULL);
    case type::Type::TypeId::SMALLINT:
      return codegen.Const16(type::PELOTON_INT16_NULL);
    case type::Type::TypeId::INTEGER:
      return codegen.Const32(type::PELOTON_INT32_NULL);
    case type::Type::TypeId::BIGINT:
      return codegen.Const64(type::PELOTON_INT64_NULL);
    case type::Type::TypeId::DECIMAL:
      return codegen.ConstDouble(type::PELOTON_DECIMAL_NULL);
    case type::Type::TypeId::TIMESTAMP:
      return codegen.Const64(type::PELOTON_TIMESTAMP_NULL);
    case type::Type::TypeId::VARBINARY:
    case type::Type::TypeId::VARCHAR:
      return codegen.Null(codegen.Int8Type());
    default: {
      auto msg = StringUtil::Format("Unknown Type '%d' for GetNullValue",
                                    static_cast<uint32_t>(type_id));
      throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, msg);
    }
  }
}

const Type::Comparison *Type::GetComparison(type::Type::TypeId type_id) {
  return kComparisonTable[static_cast<uint32_t>(type_id)];
}

const Type::BinaryOperator *Type::GetBinaryOperator(
    Type::BinaryOperatorId op_id, type::Type::TypeId left_type,
    type::Type::TypeId right_type) {
  // Get the list of function overloads
  const auto &iter = kBuiltinBinaryOperatorsTable.find(op_id);
  PL_ASSERT(iter != kBuiltinBinaryOperatorsTable.end());
  const auto &funcs = iter->second;

  for (const auto *func : funcs) {
    if (func->SupportsTypes(left_type, right_type)) {
      return func;
    }
  }

  // Error
  throw Exception{"No compatible binary operator [" +
                  kBinaryOpNames[static_cast<uint32_t>(op_id)] +
                  "] for input types: " + TypeIdToString(left_type) + ", " +
                  TypeIdToString(right_type)};
}

}  // namespace codegen
}  // namespace peloton
