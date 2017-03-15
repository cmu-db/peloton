//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value.cpp
//
// Identification: src/codegen/value.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/value.h"

#include "codegen/if.h"
#include "codegen/runtime_functions_proxy.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
Value::Value() : Value(type::Type::TypeId::INVALID) {}

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
Value::Value(type::Type::TypeId type, llvm::Value *val, llvm::Value *length,
             llvm::Value *null)
    : type_(type), value_(val), length_(length), null_(null) {}

//===----------------------------------------------------------------------===//
// Generate code to check value equality
//===----------------------------------------------------------------------===//
Value Value::CompareEq(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpUEQ(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpEQ(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// Generate code to check value inequality
//===----------------------------------------------------------------------===//
Value Value::CompareNe(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpUNE(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpNE(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// Generate code to check if the current value is less than the one provided
//===----------------------------------------------------------------------===//
Value Value::CompareLt(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpULT(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpSLT(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// Generate code to check if the current value is greater than the one provided
//===----------------------------------------------------------------------===//
Value Value::CompareLte(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpULE(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpSLE(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// Generate code to check if the current value is greater than the one provided
//===----------------------------------------------------------------------===//
Value Value::CompareGt(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpUGT(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpSGT(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// Generate code to check if the current value is greater than or equal to the
// one provided
//===----------------------------------------------------------------------===//
Value Value::CompareGte(CodeGen &codegen, const Value &o) const {
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  // Do the comparison
  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFCmpUGE(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateICmpSGE(op_md.lhs_val, op_md.rhs_val);
  }

  return Value{type::Type::TypeId::BOOLEAN, res};
}

//===----------------------------------------------------------------------===//
// This comparison function is more expensive than the other Compre* functions.
// We return:
//  => -1 if this value is less than the other
//  =>  0 if this value is equal to the other
//  =>  1 if this value is greater than the other
//
// To do this, we perform both a CompareLt and a CompareGt.
//===----------------------------------------------------------------------===//
Value Value::CompareForSort(CodeGen &codegen, const codegen::Value &o) const {
  auto lt_res = CompareLt(codegen, o);
  auto gt_res = CompareGt(codegen, o);
  auto lt_ret = codegen->CreateSelect(lt_res.GetValue(), codegen.Const32(-1),
                                      codegen.Const32(0));
  auto gt_ret = codegen->CreateSelect(gt_res.GetValue(), codegen.Const32(1),
                                      codegen.Const32(0));
  return Value{type::Type::TypeId::INTEGER, codegen->CreateOr(lt_ret, gt_ret)};
}

//===----------------------------------------------------------------------===//
// Mathematical addition with overflow checking
//===----------------------------------------------------------------------===//
Value Value::Add(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);

  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFAdd(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateAdd(op_md.lhs_val, op_md.rhs_val);
  }

  //  // If the input types are different, promote to the largest type
  //  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);
  //
  //  // Use the intrinsic sadd function so that we can check for overflow
  //  llvm::Function *sadd_func = llvm::Intrinsic::getDeclaration(
  //      &codegen.GetModule(), llvm::Intrinsic::sadd_with_overflow,
  //      op_md.llvm_type);
  //  PL_ASSERT(sadd_func != nullptr);
  //
  //  // returns a struct: { addition_result, overflow bit}
  //  llvm::Value *result_struct =
  //      codegen.CallFunc(sadd_func, {op_md.lhs_val, op_md.rhs_val});
  //  llvm::Value *add_res = codegen->CreateExtractValue(result_struct, 0);
  //  llvm::Value *overflow_bit = codegen->CreateExtractValue(result_struct, 1);
  //
  //  // Throw exception if we encountered overflow
  //  CreateCheckOverflowException(codegen, overflow_bit);
  //
  //  return Value{op_md.value_type, add_res};
  // return Value{GetType(), codegen->CreateAdd(GetValue(), o.GetValue())};
  return Value{op_md.value_type, res};
}

//===----------------------------------------------------------------------===//
// Mathematical subtraction with overflow checking
//===----------------------------------------------------------------------===//
Value Value::Sub(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);

  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFSub(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateSub(op_md.lhs_val, op_md.rhs_val);
  }

  //
  //  // Use the intrinsic ssub function so that we can check for overflow
  //  llvm::Function *ssub_func = llvm::Intrinsic::getDeclaration(
  //      &codegen.GetModule(), llvm::Intrinsic::ssub_with_overflow,
  //      op_md.llvm_type);
  //  PL_ASSERT(ssub_func != nullptr);
  //
  //  // returns a struct: { subtraction_result, overflow bit}
  //  llvm::Value *result_struct =
  //      codegen.CallFunc(ssub_func, {op_md.lhs_val, op_md.rhs_val});
  //  llvm::Value *sub_res = codegen->CreateExtractValue(result_struct, 0);
  //  llvm::Value *overflow_bit = codegen->CreateExtractValue(result_struct, 1);
  //
  //  // Throw exception if we encountered overflow
  //  CreateCheckOverflowException(codegen, overflow_bit);
  //
  //  return Value{op_md.value_type, sub_res};
  //  return Value{GetType(), codegen->CreateSub(GetValue(), o.GetValue())};
  return Value{op_md.value_type, res};
}

//===----------------------------------------------------------------------===//
// Mathematical multiplication with overflow checking
//===----------------------------------------------------------------------===//
Value Value::Mul(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);

  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFMul(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateMul(op_md.lhs_val, op_md.rhs_val);
  }

  //
  //  // Use the intrinsic smul function so that we can check for overflow
  //  llvm::Function *smul_func = llvm::Intrinsic::getDeclaration(
  //      &codegen.GetModule(), llvm::Intrinsic::smul_with_overflow,
  //      op_md.llvm_type);
  //  PL_ASSERT(smul_func != nullptr);
  //
  //  // returns a struct: { multiplication_result, overflow bit}
  //  llvm::Value *result_struct =
  //      codegen.CallFunc(smul_func, {op_md.lhs_val, op_md.rhs_val});
  //  llvm::Value *mul_res = codegen->CreateExtractValue(result_struct, 0);
  //  llvm::Value *overflow_bit = codegen->CreateExtractValue(result_struct, 1);
  //
  //  // Throw exception if we encountered overflow
  //  CreateCheckOverflowException(codegen, overflow_bit);
  //
  //  return Value{op_md.value_type, mul_res};
  //  return Value{GetType(), codegen->CreateMul(op_md.lhs_val, op_md.rhs_val)};
  return Value{op_md.value_type, res};
}

//===----------------------------------------------------------------------===//
// Mathematical division
//===----------------------------------------------------------------------===//
Value Value::Div(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);

  //  // Throw an exception if the divisor is zero
  //  CreateCheckDivideByZeroException(codegen, o);
  //
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);

  llvm::Value *res = nullptr;
  if (op_md.value_type == type::Type::TypeId::DECIMAL) {
    res = codegen->CreateFDiv(op_md.lhs_val, op_md.rhs_val);
  } else {
    res = codegen->CreateSDiv(op_md.lhs_val, op_md.rhs_val);
  }

  //
  //  // Check for overflow. There is no intrinsic function to do this
  //  // (like there is for add/sub/mul) so we do it manually.
  //  //
  //  // Overflow occurs if: dividend == INT_MIN && divisor == -1
  //
  //  // Build predicate
  //  llvm::Constant *minval = llvm::ConstantInt::get(
  //      op_md.llvm_type,
  //      llvm::APInt::getSignedMinValue(
  //          static_cast<llvm::IntegerType
  //          *>(op_md.llvm_type)->getBitWidth()));
  //  llvm::Constant *neg_one = llvm::ConstantInt::get(op_md.llvm_type, -1);
  //  llvm::Value *lhs_eq_minval = codegen->CreateICmpEQ(op_md.lhs_val, minval);
  //  llvm::Value *rhs_eq_neg_one = codegen->CreateICmpEQ(op_md.rhs_val,
  //  neg_one);
  //  llvm::Value *conj = codegen->CreateAnd(lhs_eq_minval, rhs_eq_neg_one);
  //
  //  // Add if block to check for overflow
  //  If has_overflow{codegen, conj};
  //  {
  //    // Throw OverflowException
  //    codegen.CallFunc(
  //        RuntimeFunctionsProxy::_ThrowOverflowException::GetFunction(codegen),
  //        {});
  //  }
  //  has_overflow.EndIf();
  //
  //  // Do the division
  //  return Value{op_md.value_type,
  //               codegen->CreateSDiv(op_md.lhs_val, op_md.rhs_val)};
  //  return Value{GetType(), codegen->CreateSDiv(GetValue(), o.GetValue())};
  return Value{op_md.value_type, res};
}

//===----------------------------------------------------------------------===//
// Mathematical modulus
//===----------------------------------------------------------------------===//
Value Value::Mod(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);

  //  // Throw an exception if the divisor is zero
  //  CreateCheckDivideByZeroException(codegen, o);
  //
  // If the input types are different, promote to the largest type
  OpPromotionMetadata op_md = Value::PromoteOperands(codegen, *this, o);
  //
  //  // Do the mod
  return Value{op_md.value_type,
               codegen->CreateSRem(op_md.lhs_val, op_md.rhs_val)};
  //  return Value{GetType(), codegen->CreateSRem(GetValue(), o.GetValue())};
}

//===----------------------------------------------------------------------===//
// Mathematical minimum
//===----------------------------------------------------------------------===//
Value Value::Min(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);
  PL_ASSERT(GetType() == o.GetType());

  llvm::Value *comparison = codegen->CreateICmpSLT(GetValue(), o.GetValue());
  return Value{GetType(),
               codegen->CreateSelect(comparison, GetValue(), o.GetValue())};
}

//===----------------------------------------------------------------------===//
// Mathematical maximum
//===----------------------------------------------------------------------===//
Value Value::Max(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() != type::Type::TypeId::INVALID);
  PL_ASSERT(GetType() != type::Type::TypeId::VARCHAR);
  PL_ASSERT(GetType() == o.GetType());

  llvm::Value *comparison = codegen->CreateICmpSLT(GetValue(), o.GetValue());
  return Value{GetType(),
               codegen->CreateSelect(comparison, o.GetValue(), GetValue())};
}

//===----------------------------------------------------------------------===//
// Logical AND
//===----------------------------------------------------------------------===//
Value Value::LogicalAnd(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == o.GetType());

  llvm::Value *land = codegen->CreateAnd(GetValue(), o.GetValue());
  return Value{GetType(), land};
}

//===----------------------------------------------------------------------===//
// Logical OR
//===----------------------------------------------------------------------===//
Value Value::LogicalOr(CodeGen &codegen, const Value &o) const {
  PL_ASSERT(GetType() == type::Type::TypeId::BOOLEAN);
  PL_ASSERT(GetType() == o.GetType());

  llvm::Value *lor = codegen->CreateOr(GetValue(), o.GetValue());
  return Value{GetType(), lor};
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForHash(__attribute((unused)) CodeGen &codegen,
                          llvm::Value *&val, llvm::Value *&len) const {
  val = GetValue();
  if (GetType() == type::Type::TypeId::VARCHAR ||
      GetType() == type::Type::TypeId::VARBINARY) {
    len = GetLength();
  } else {
    len = nullptr;
  }
}

//===----------------------------------------------------------------------===//
// Generate a hash for the given value
//===----------------------------------------------------------------------===//
void Value::ValuesForMaterialization(__attribute((unused)) CodeGen &codegen,
                                     llvm::Value *&val,
                                     llvm::Value *&len) const {
  PL_ASSERT(type_ != type::Type::TypeId::INVALID);
  val = GetValue();
  len = GetType() == type::Type::TypeId::VARCHAR ? GetLength() : nullptr;
}

//===----------------------------------------------------------------------===//
// Return the types of this value suitable for materialization.  The types
// should be such that
//===----------------------------------------------------------------------===//
void Value::TypeForMaterialization(CodeGen &codegen,
                                   type::Type::TypeId value_type,
                                   llvm::Type *&val_type,
                                   llvm::Type *&len_type) {
  PL_ASSERT(value_type != type::Type::TypeId::INVALID);
  switch (value_type) {
    case type::Type::TypeId::BOOLEAN:
      val_type = codegen.BoolType();
      break;
    case type::Type::TypeId::TINYINT:
      val_type = codegen.Int8Type();
      break;
    case type::Type::TypeId::SMALLINT:
      val_type = codegen.Int16Type();
      break;
    case type::Type::TypeId::INTEGER:
      val_type = codegen.Int32Type();
      break;
    case type::Type::TypeId::BIGINT:
      val_type = codegen.Int64Type();
      break;
    case type::Type::TypeId::DECIMAL:
      val_type = codegen.DoubleType();
      break;
    case type::Type::TypeId::VARCHAR:
      val_type = codegen.CharPtrType();
      len_type = codegen.Int32Type();
      break;
    default: {
      throw std::runtime_error{"We can't materialize" +
                               std::to_string(value_type) + "yet"};
    }
  }
}

//===----------------------------------------------------------------------===//
// Return a value that can be constructed from the provided type and value
// registers
//===----------------------------------------------------------------------===//
Value Value::ValueFromMaterialization(__attribute((unused)) CodeGen &codegen,
                                      type::Type::TypeId type,
                                      llvm::Value *&val, llvm::Value *&len) {
  PL_ASSERT(type != type::Type::TypeId::INVALID);
  return Value{type, val, type == type::Type::TypeId::VARCHAR ? len : nullptr};
}

//===----------------------------------------------------------------------===//
// Get the LLVM type that matches the numeric type provided
//===----------------------------------------------------------------------===//
llvm::Type *Value::NumericType(CodeGen &codegen, type::Type::TypeId type) {
  switch (type) {
    case type::Type::TypeId::BOOLEAN:
      return codegen.BoolType();
    case type::Type::TypeId::TINYINT:
      return codegen.Int8Type();
    case type::Type::TypeId::SMALLINT:
      return codegen.Int16Type();
    case type::Type::TypeId::DATE:
    case type::Type::TypeId::INTEGER:
      return codegen.Int32Type();
    case type::Type::TypeId::TIMESTAMP:
    case type::Type::TypeId::BIGINT:
      return codegen.Int64Type();
    case type::Type::TypeId::DECIMAL:
      return codegen.DoubleType();
    default:
      throw std::runtime_error{std::to_string(type) + " is not a numeric"};
  }
}

//===----------------------------------------------------------------------===//
// // Return the promoted LLVM values and type info for the <lhs,rhs> ops
//===----------------------------------------------------------------------===//
Value::OpPromotionMetadata Value::PromoteOperands(CodeGen &codegen,
                                                  const Value &lhs,
                                                  const Value &rhs) {
  Value::OpPromotionMetadata op_metadata;

  // TODO: Fix me
  op_metadata.value_type = std::max(lhs.GetType(), rhs.GetType());
  op_metadata.llvm_type = NumericType(codegen, op_metadata.value_type);

  // Promote LHS if necessary
  op_metadata.lhs_val = lhs.GetValue();
  if (lhs.GetType() != op_metadata.value_type) {
    op_metadata.lhs_val = codegen->CreateSExtOrBitCast(op_metadata.lhs_val,
                                                       op_metadata.llvm_type);
  }

  // Promote RHS if necessary
  op_metadata.rhs_val = rhs.GetValue();
  if (rhs.GetType() != op_metadata.value_type) {
    op_metadata.rhs_val = codegen->CreateSExtOrBitCast(op_metadata.rhs_val,
                                                       op_metadata.llvm_type);
  }
  return op_metadata;
}

//===----------------------------------------------------------------------===//
// Build a new value that combines values arriving from different BB's into a
// single value.
//===----------------------------------------------------------------------===//
Value Value::BuildPHI(
    CodeGen &codegen,
    const std::vector<std::pair<Value, llvm::BasicBlock *>> &vals) {
  PL_ASSERT(vals.size() > 0);
  auto type = vals[0].first.GetType();
  if (type == type::Type::TypeId::VARCHAR ||
      type == type::Type::TypeId::VARBINARY) {
    auto *val_phi = codegen->CreatePHI(codegen.CharPtrType(), vals.size());
    auto *len_phi = codegen->CreatePHI(codegen.Int32Type(), vals.size());
    for (const auto &val_pair : vals) {
      val_phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
      len_phi->addIncoming(val_pair.first.GetLength(), val_pair.second);
    }
    return Value{type, val_phi, len_phi};
  } else {
    auto *phi = codegen->CreatePHI(NumericType(codegen, type), vals.size());
    for (const auto &val_pair : vals) {
      phi->addIncoming(val_pair.first.GetValue(), val_pair.second);
    }
    return Value{type, phi, nullptr};
  }
}

//===----------------------------------------------------------------------===//
// Create a conditional branch to throw a divide by zero exception
//===----------------------------------------------------------------------===//
void Value::CreateCheckDivideByZeroException(CodeGen &codegen,
                                             const codegen::Value &divisor) {
  // Check if divisor is zero
  llvm::Constant *zero =
      llvm::ConstantInt::get(NumericType(codegen, divisor.GetType()), 0, true);
  If val_is_zero{codegen, codegen->CreateICmpEQ(divisor.GetValue(), zero)};
  {
    // Throw DivideByZeroException
    codegen.CallFunc(
        RuntimeFunctionsProxy::_ThrowDivideByZeroException::GetFunction(
            codegen),
        {});
  }
  val_is_zero.EndIf();
}

//===----------------------------------------------------------------------===//
// Create a conditional branch to throw an overflow exception
//===----------------------------------------------------------------------===//
void Value::CreateCheckOverflowException(CodeGen &codegen,
                                         llvm::Value *overflow_bit) {
  // Generate if statement to check for overflow
  If has_overflow{codegen,
                  codegen->CreateICmpEQ(overflow_bit, codegen.ConstBool(1))};
  {
    // Throw overflow exception if the overflow bit is set
    codegen.CallFunc(
        RuntimeFunctionsProxy::_ThrowOverflowException::GetFunction(codegen),
        {});
  }
  has_overflow.EndIf();
}

}  // namespace codegen
}  // namespace peloton