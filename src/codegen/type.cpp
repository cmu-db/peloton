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

#include "include/codegen/util/if.h"
#include "include/codegen/proxy/values_runtime_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Type casting
//===----------------------------------------------------------------------===//

struct InvalidCast : public Type::Cast {
  Value DoCast(UNUSED_ATTRIBUTE CodeGen &codegen, const Value &value,
               type::Type::TypeId to_type) const override {
    std::string msg = StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str());
    throw NotImplementedException{msg};
  }
};

//===----------------------------------------------------------------------===//
// Boolean casting rules
//===----------------------------------------------------------------------===//

struct CastBoolean : public Type::Cast {
  // Cast the boolean value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::Type::TypeId to_type) const override {
    PL_ASSERT(Type::IsIntegral(value.GetType()));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (Type::IsIntegral(to_type)) {
      // Any integral value requires a zero-extension
      return Value{to_type, codegen->CreateZExt(value.GetValue(), val_type)};
    } else if (Type::IsNumeric(to_type)) {
      // Convert this boolean (unsigned int) into the double type
      return Value{to_type, codegen->CreateUIToFP(value.GetValue(), val_type)};
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
//===----------------------------------------------------------------------===//

struct CastInteger : public Type::Cast {
  // Cast the given integer value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::Type::TypeId to_type) const override {
    PL_ASSERT(Type::IsIntegral(value.GetType()));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (Type::IsIntegral(to_type)) {
      // For integral casts, we need to either truncate or sign-extend
      uint32_t curr_size = Type::GetFixedSizeForType(value.GetType());
      uint32_t target_size = Type::GetFixedSizeForType(to_type);

      llvm::Value *res = nullptr;
      if (curr_size == target_size) {
        res = value.GetValue();
      } else if (curr_size < target_size) {
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
//===----------------------------------------------------------------------===//

struct CastDecimal : public Type::Cast {
  // Cast the given decimal value into the provided type
  Value DoCast(CodeGen &codegen, const Value &value,
               type::Type::TypeId to_type) const override {
    PL_ASSERT(Type::IsNumeric(value.GetType()));

    // Types for the casted-to type
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, to_type, val_type, len_type);

    if (Type::IsIntegral(to_type)) {
      // Convert the decimal value into an integral value
      llvm::Value *res = codegen->CreateFPToSI(value.GetValue(), val_type);

      // We're done
      return Value{to_type, res};
    } else if (Type::IsNumeric(to_type)) {
      // Just return the value
      return value;
    }

    // TODO: Fill me out
    // Can't do anything else for now
    throw NotImplementedException{StringUtil::Format(
        "Cannot cast %s to %s", TypeIdToString(value.GetType()).c_str(),
        TypeIdToString(to_type).c_str())};
  }
};

//===----------------------------------------------------------------------===//
// Default Comparison
//===----------------------------------------------------------------------===//

// Main comparison functions
Value Type::Comparison::DoCompareLt(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid LT comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoCompareLte(UNUSED_ATTRIBUTE CodeGen &codegen,
                                     const Value &left,
                                     const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid LTE comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoCompareEq(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid EQ comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoCompareNe(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid NE comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoCompareGt(UNUSED_ATTRIBUTE CodeGen &codegen,
                                    const Value &left,
                                    const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid GT comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoCompareGte(UNUSED_ATTRIBUTE CodeGen &codegen,
                                     const Value &left,
                                     const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid GTE comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
}

Value Type::Comparison::DoComparisonForSort(UNUSED_ATTRIBUTE CodeGen &codegen,
                                            const Value &left,
                                            const Value &right) const {
  std::string msg =
      StringUtil::Format("Invalid SORT comparison between types %s and %s",
                         TypeIdToString(left.GetType()).c_str(),
                         TypeIdToString(right.GetType()).c_str());
  throw Exception{EXCEPTION_TYPE_NOT_IMPLEMENTED, msg};
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
  void CastToCorrectType(CodeGen &codegen, const Value &left,
                         const Value &right, Value &proper_left,
                         Value &proper_right) const {
    proper_left = left;
    proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }
  }

  Value DoCompareLt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    // Cast appropriately
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSLT(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareLte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSLE(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareEq(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpEQ(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareNe(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpNE(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGt(CodeGen &codegen, const Value &left,
                    const Value &right) const override {
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSGT(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoCompareGte(CodeGen &codegen, const Value &left,
                     const Value &right) const override {
    Value proper_left, proper_right;
    CastToCorrectType(codegen, left, right, proper_left, proper_right);

    // Do comparison
    llvm::Value *result =
        codegen->CreateICmpSGE(proper_left.GetValue(), proper_right.GetValue());

    // Return the result
    return Value{type::Type::TypeId::BOOLEAN, result};
  }

  Value DoComparisonForSort(CodeGen &codegen, const Value &left,
                            const Value &right) const override {
    // For integer comparisons, just subtract left from right and cast the
    // result to a 32-bit value
    Value sub_result = left.Sub(codegen, right);
    return sub_result.CastTo(codegen, type::Type::TypeId::INTEGER);
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

    // Setup the function arguments and invoke the call
    std::vector<llvm::Value *> args = {left.GetValue(), left.GetLength(),
                                       right.GetValue(), right.GetLength()};
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
    result = codegen->CreateICmpSGE(result, codegen.Const32(0));

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
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    // The convention for this function is: sizeof(left) >= sizeof(right)

    codegen::Value proper_left = left;
    codegen::Value proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }

    // Do addition
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallAddWithOverflow(
        proper_left.GetValue(), proper_right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{proper_left.GetType(), result};
  }
};

struct IntegerSub : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    // The convention for this function is: sizeof(left) >= sizeof(right)

    codegen::Value proper_left = left;
    codegen::Value proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }

    // Do subtraction
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallSubWithOverflow(
        proper_left.GetValue(), proper_right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{proper_left.GetType(), result};
  }
};

struct IntegerMul : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    codegen::Value proper_left = left;
    codegen::Value proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }

    // Do multiplication
    llvm::Value *overflow_bit = nullptr;
    llvm::Value *result = codegen.CallMulWithOverflow(
        proper_left.GetValue(), proper_right.GetValue(), overflow_bit);

    if (on_error == Value::OnError::Exception) {
      codegen.ThrowIfOverflow(overflow_bit);
    }

    // Return result
    return Value{proper_left.GetType(), result};
  }
};

struct IntegerDiv : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    codegen::Value proper_left = left;
    codegen::Value proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }

    // First, check if the divisor is zero
    codegen::Value div0 = right.CompareEq(
        codegen, Value{type::Type::TypeId::INTEGER, codegen.Const32(0)});

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
              Type::GetDefaultValue(codegen, proper_left.GetType()).GetValue();
        } else {
          default_val =
              Type::GetNullValue(codegen, proper_left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result = codegen->CreateSDiv(proper_left.GetValue(),
                                              proper_right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do division
      result =
          codegen->CreateSDiv(proper_left.GetValue(), proper_right.GetValue());
    }

    // Return result
    return Value{proper_left.GetType(), result};
  }
};

struct IntegerMod : public Type::BinaryOperator {
  bool SupportsTypes(type::Type::TypeId left_type,
                     type::Type::TypeId right_type) const override {
    return Type::IsIntegral(left_type) && Type::IsIntegral(right_type);
  }

  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    codegen::Value proper_left = left;
    codegen::Value proper_right = right;

    // If the types don't match, we'll need to do some casting
    if (left.GetType() != right.GetType()) {
      uint32_t left_size = Type::GetFixedSizeForType(left.GetType());
      uint32_t right_size = Type::GetFixedSizeForType(right.GetType());
      if (left_size < right_size) {
        proper_left = left.CastTo(codegen, right.GetType());
      } else {
        proper_right = right.CastTo(codegen, left.GetType());
      }
    }

    // First, check if the divisor is zero
    codegen::Value div0 = right.CompareEq(
        codegen, Value{type::Type::TypeId::INTEGER, codegen.Const32(0)});

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
              Type::GetDefaultValue(codegen, proper_left.GetType()).GetValue();
        } else {
          default_val =
              Type::GetNullValue(codegen, proper_left.GetType()).GetValue();
        }
      }
      is_div0.ElseBlock();
      {
        division_result = codegen->CreateSRem(proper_left.GetValue(),
                                              proper_right.GetValue());
      }
      is_div0.EndIf();

      // Build PHI
      result = is_div0.BuildPHI(default_val, division_result);
    } else if (on_error == Value::OnError::Exception) {
      // If the caller **does** care about the error, generate the exception
      codegen.ThrowIfDivideByZero(div0.GetValue());

      // Do division
      result =
          codegen->CreateSRem(proper_left.GetValue(), proper_right.GetValue());
    }

    // Return result
    return Value{proper_left.GetType(), result};
  }
};

//===----------------------------------------------------------------------===//
// DECIMAL OPS
//
// This class assumes the left value is a decimal value
//===----------------------------------------------------------------------===//

struct DecimalAdd : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    (void)on_error;
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFAdd(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalSub : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    (void)on_error;
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFSub(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalMul : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    (void)on_error;
    PL_ASSERT(left.GetType() == right.GetType());

    // Do addition
    llvm::Value *result =
        codegen->CreateFMul(left.GetValue(), right.GetValue());

    // Return result
    return Value{left.GetType(), result};
  }
};

struct DecimalDiv : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    PL_ASSERT(left.GetType() == right.GetType());

    // First, check if the divisor is zero
    codegen::Value div0 = right.CompareEq(
        codegen, Value{type::Type::TypeId::DECIMAL, codegen.ConstDouble(0.0)});

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
    return Value{left.GetType(), result};
  }
};

struct DecimalMod : public Type::BinaryOperator {
  Value DoWork(CodeGen &codegen, const Value &left, const Value &right,
               Value::OnError on_error) const override {
    (void)on_error;
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

static InvalidCast kInvalidCast;
static CastBoolean kCastBoolean;
static CastInteger kCastInteger;
static CastDecimal kCastDecimal;

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

static DecimalAdd kDecimalAdd;
static DecimalSub kDecimalSub;
static DecimalMul kDecimalMul;
static DecimalDiv kDecimalDiv;
static DecimalMod kDecimalMod;

// String representation of all binary operators
const std::string Type::kOpNames[] = {"Negation", "Abs", "Add", "Sub",
                                      "Mul",      "Div", "Mod"};

std::vector<const Type::Cast *> Type::kCastingTable = {
    &kInvalidCast,   // Invalid
    &kInvalidCast,   // Param offset
    &kCastBoolean,   // Boolean
    &kCastInteger,   // Tiny Int
    &kCastInteger,   // Small Int
    &kCastInteger,   // Integer
    &kCastInteger,   // BigInt
    &kCastDecimal,   // Decimal
    &kInvalidCast,   // Timestamp
    &kInvalidCast,   // Date
    &kInvalidCast,   // Varchar
    &kInvalidCast,   // Varbinary
    &kInvalidCast,   // Array
    &kInvalidCast};  // UDT

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
    {Type::OperatorId::Add, {&kIntegerAdd, &kDecimalAdd}},
    {Type::OperatorId::Sub, {&kIntegerSub, &kDecimalSub}},
    {Type::OperatorId::Mul, {&kIntegerMul, &kDecimalMul}},
    {Type::OperatorId::Div, {&kIntegerDiv, &kDecimalDiv}},
    {Type::OperatorId::Mod, {&kIntegerMod, &kDecimalMod}}};

// Get the number of bytes needed to store the given type
uint32_t Type::GetFixedSizeForType(type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
    case type::Type::TypeId::TINYINT:
      return 1;
    case type::Type::TypeId::SMALLINT:
      return 2;
    case type::Type::TypeId::INTEGER:
    case type::Type::TypeId::DATE:
      return 4;
    case type::Type::TypeId::BIGINT:
    case type::Type::TypeId::DECIMAL:
    case type::Type::TypeId::TIMESTAMP:
    case type::Type::TypeId::VARCHAR:
    case type::Type::TypeId::VARBINARY:
    case type::Type::TypeId::ARRAY:
      return 8;
    default:
      break;
  }
  std::string msg = StringUtil::Format("Type '%s' doesn't have a fixed size",
                                       TypeIdToString(type_id).c_str());
  throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
}

bool Type::HasVariableLength(type::Type::TypeId type_id) {
  return type_id == type::Type::TypeId::VARCHAR ||
         type_id == type::Type::TypeId::VARBINARY;
}

bool Type::IsIntegral(type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::TINYINT:
    case type::Type::TypeId::SMALLINT:
    case type::Type::TypeId::DATE:
    case type::Type::TypeId::INTEGER:
    case type::Type::TypeId::TIMESTAMP:
    case type::Type::TypeId::BIGINT:
      return true;
    default:
      return false;
  }
}

bool Type::IsNumeric(type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::DECIMAL:
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
      throw Exception{StringUtil::Format("'%s' is not a materializable type",
                                         TypeIdToString(type_id).c_str())};
    }
  }
}

Value Type::GetMinValue(CodeGen &codegen, type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_MIN)};
    case type::Type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_MIN)};
    case type::Type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_MIN)};
    case type::Type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_MIN)};
    case type::Type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_MIN)};
    case type::Type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_MIN)};
    case type::Type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_MIN)};
    default: {
      auto msg = StringUtil::Format("No minimum value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetMaxValue(CodeGen &codegen, type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_MAX)};
    case type::Type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_MAX)};
    case type::Type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_MAX)};
    case type::Type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_MAX)};
    case type::Type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_MAX)};
    case type::Type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_MAX)};
    case type::Type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_MAX)};
    default: {
      auto msg = StringUtil::Format("No maximum value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetNullValue(CodeGen &codegen, type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(type::PELOTON_BOOLEAN_NULL)};
    case type::Type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(type::PELOTON_INT8_NULL)};
    case type::Type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(type::PELOTON_INT16_NULL)};
    case type::Type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(type::PELOTON_INT32_NULL)};
    case type::Type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(type::PELOTON_INT64_NULL)};
    case type::Type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(type::PELOTON_DECIMAL_NULL)};
    case type::Type::TypeId::DATE:
      return Value{type_id, codegen.Const32(type::PELOTON_DATE_NULL)};
    case type::Type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(type::PELOTON_TIMESTAMP_NULL)};
    case type::Type::TypeId::VARBINARY:
    case type::Type::TypeId::VARCHAR:
      return Value{type_id, codegen.NullPtr(codegen.CharPtrType()),
                   codegen.Const32(0)};
    default: {
      auto msg = StringUtil::Format("No null value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

Value Type::GetDefaultValue(CodeGen &codegen, type::Type::TypeId type_id) {
  switch (type_id) {
    case type::Type::TypeId::BOOLEAN:
      return Value{type_id, codegen.ConstBool(false)};
    case type::Type::TypeId::TINYINT:
      return Value{type_id, codegen.Const8(0)};
    case type::Type::TypeId::SMALLINT:
      return Value{type_id, codegen.Const16(0)};
    case type::Type::TypeId::INTEGER:
      return Value{type_id, codegen.Const32(0)};
    case type::Type::TypeId::BIGINT:
      return Value{type_id, codegen.Const64(0)};
    case type::Type::TypeId::DECIMAL:
      return Value{type_id, codegen.ConstDouble(0.0)};
    case type::Type::TypeId::DATE:
      return Value{type_id, codegen.Const32(0)};
    case type::Type::TypeId::TIMESTAMP:
      return Value{type_id, codegen.Const64(0)};
    default: {
      auto msg = StringUtil::Format("No default value for type '%s'",
                                    TypeIdToString(type_id).c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, msg};
    }
  }
}

const Type::Cast *Type::GetCast(type::Type::TypeId from_type,
                                type::Type::TypeId to_type) {
  (void)to_type;
  return kCastingTable[static_cast<uint32_t>(from_type)];
}

const Type::Comparison *Type::GetComparison(type::Type::TypeId type_id) {
  return kComparisonTable[static_cast<uint32_t>(type_id)];
}

const Type::BinaryOperator *Type::GetBinaryOperator(
    Type::OperatorId op_id, type::Type::TypeId left_type,
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
  std::string msg = StringUtil::Format(
      "No compatible '%s' operator for input types: %s, %s",
      kOpNames[static_cast<uint32_t>(op_id)].c_str(),
      TypeIdToString(left_type).c_str(), TypeIdToString(right_type).c_str());
  throw Exception{msg};
}

}  // namespace codegen
}  // namespace peloton
