//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_system.cpp
//
// Identification: src/codegen/type/type_system.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type/type_system.h"

#include "codegen/lang/if.h"
#include "codegen/value.h"
#include "codegen/type/boolean_type.h"
#include "common/exception.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace type {

bool TypeSystem::CastWithNullPropagation::SupportsTypes(
    const Type &from_type, const Type &to_type) const {
  return inner_cast_.SupportsTypes(from_type, to_type);
}

Value TypeSystem::CastWithNullPropagation::DoCast(CodeGen &codegen,
                                                  const Value &value,
                                                  const Type &to_type) const {
  PL_ASSERT(value.IsNullable());

  Value null_val, ret_val;
  lang::If is_null{codegen, value.IsNull(codegen)};
  {
    // If the value is NULL, return the NULL type for the target type
    null_val = to_type.GetSqlType().GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If both values are not null, perform the non-null-aware operation
    ret_val = inner_cast_.DoCast(codegen, value, to_type);
  }
  is_null.EndIf();

  return is_null.BuildPHI(null_val, ret_val);
}

//===----------------------------------------------------------------------===//
// ComparisonWithNullPropagation
//
// This is a wrapper around lower-level comparisons that are not null-aware.
// This class computes the null-bit of the result of the comparison based on the
// values being compared. It delegates to the wrapped comparison function to
// perform the actual comparison. The null-bit and resulting value are combined.
//===----------------------------------------------------------------------===//

#define DO_COMPARE(OP)                                                     \
  PL_ASSERT(left.IsNullable() || right.IsNullable());                      \
  /* Determine the null bit based on the left and right values */          \
  llvm::Value *null = nullptr;                                             \
  if (left.IsNullable() && right.IsNullable()) {                           \
    null = codegen->CreateOr(left.IsNull(codegen), right.IsNull(codegen)); \
  } else if (left.IsNullable()) {                                          \
    null = left.IsNull(codegen);                                           \
  } else {                                                                 \
    null = right.IsNull(codegen);                                          \
  }                                                                        \
  /* Now perform the comparison using a non-null-aware comparison */       \
  Value result = (OP);                                                     \
  /* Return the result with the computed null-bit */                       \
  return Value{result.GetType().AsNullable(), result.GetValue(), nullptr, null};

bool TypeSystem::ComparisonWithNullPropagation::SupportsTypes(
    const Type &left_type, const Type &right_type) const {
  return inner_comparison_.SupportsTypes(left_type, right_type);
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareLt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareLt(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareLte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareLte(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareEq(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareEq(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareNe(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareNe(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareGt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareGt(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoCompareGte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoCompareGte(codegen, left, right));
}

Value TypeSystem::ComparisonWithNullPropagation::DoComparisonForSort(
    CodeGen &codegen, const Value &left, const Value &right) const {
  DO_COMPARE(inner_comparison_.DoComparisonForSort(codegen, left, right));
}

#undef DO_COMPARE

//===----------------------------------------------------------------------===//
// UnaryOperatorWithNullPropagation
//
// This is a wrapper around lower-level unary operators which are not
// null-aware. This class properly computes the result of a unary operator in
// the presence of null input values.
//===----------------------------------------------------------------------===//

bool TypeSystem::UnaryOperatorWithNullPropagation::SupportsType(
    const Type &type) const {
  return inner_op_.SupportsType(type);
}

Type TypeSystem::UnaryOperatorWithNullPropagation::ResultType(
    const Type &val_type) const {
  return inner_op_.ResultType(val_type).AsNullable();
}

Value TypeSystem::UnaryOperatorWithNullPropagation::DoWork(
    CodeGen &codegen, const Value &val) const {
  PL_ASSERT(val.IsNullable());

  Value null_val, ret_val;
  lang::If is_null{codegen, val.IsNull(codegen)};
  {
    // If the value is NULL, return the NULL type
    null_val = val.GetType().GetSqlType().GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If the input isn't NULL, perform the non-null-aware operation
    ret_val = inner_op_.DoWork(codegen, val);
  }
  is_null.EndIf();

  return is_null.BuildPHI(null_val, ret_val);
}

//===----------------------------------------------------------------------===//
// BinaryOperatorWithNullPropagation
//
// This is a wrapper around lower-level binary operators which are not
// null-aware. This class properly computes the result of a binary operator in
// the presence of null input values.
//===----------------------------------------------------------------------===//

bool TypeSystem::BinaryOperatorWithNullPropagation::SupportsTypes(
    const Type &left_type, const Type &right_type) const {
  return inner_op_.SupportsTypes(left_type, right_type);
}

Type TypeSystem::BinaryOperatorWithNullPropagation::ResultType(
    const Type &left_type, const Type &right_type) const {
  return inner_op_.ResultType(left_type, right_type).AsNullable();
}

Value TypeSystem::BinaryOperatorWithNullPropagation::DoWork(
    CodeGen &codegen, const Value &left, const Value &right,
    OnError on_error) const {
  PL_ASSERT(left.IsNullable() || right.IsNullable());

  // One of the inputs is nullable, compute the null bit first
  auto *null = codegen->CreateOr(left.IsNull(codegen), right.IsNull(codegen));

  Value null_val, ret_val;
  lang::If is_null{codegen, null};
  {
    // If either value is null, the result of the operator is null
    const auto &result_type = ResultType(left.GetType(), right.GetType());
    null_val = result_type.GetSqlType().GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If both values are not null, perform the non-null-aware operation
    ret_val = inner_op_.DoWork(codegen, left, right, on_error);
  }
  is_null.EndIf();
  return is_null.BuildPHI(null_val, ret_val);
}

TypeSystem::TypeSystem(
    const std::vector<peloton::type::TypeId> &implicit_cast_table,
    const std::vector<TypeSystem::CastInfo> &explicit_cast_table,
    const std::vector<TypeSystem::ComparisonInfo> &comparison_table,
    const std::vector<TypeSystem::UnaryOpInfo> &unary_op_table,
    const std::vector<TypeSystem::BinaryOpInfo> &binary_op_table)
    : implicit_cast_table_(implicit_cast_table),
      explicit_cast_table_(explicit_cast_table),
      comparison_table_(comparison_table),
      unary_op_table_(unary_op_table),
      binary_op_table_(binary_op_table) {}

bool TypeSystem::CanImplicitlyCastTo(const Type &from_type,
                                     const Type &to_type) {
  const auto &type_system = from_type.GetTypeSystem();
  for (auto &castable_type_id : type_system.implicit_cast_table_) {
    if (castable_type_id == to_type.type_id) {
      return true;
    }
  }
  return false;
}

const TypeSystem::Cast *TypeSystem::GetCast(const Type &from_type,
                                            const Type &to_type) {
  const auto &type_system = from_type.GetTypeSystem();
  for (const auto &cast_info : type_system.explicit_cast_table_) {
    if (cast_info.from_type == from_type.type_id &&
        cast_info.to_type == to_type.type_id) {
      return &cast_info.cast_operation;
    }
  }

  // Error
  throw CastException{from_type.type_id, to_type.type_id};
}

const TypeSystem::Comparison *TypeSystem::GetComparison(
    const Type &left_type, Type &left_casted_type, const Type &right_type,
    Type &right_casted_type) {
  const auto &left_type_system = left_type.GetTypeSystem();
  for (const auto &comparison_info : left_type_system.comparison_table_) {
    // Can we use the operation without any implicit casting?
    const auto &comparison = comparison_info.comparison;
    if (comparison.SupportsTypes(left_type, right_type)) {
      left_casted_type = left_type;
      right_casted_type = right_type;
      return &comparison;
    }

    // Check if the right input type can be casted to the left input type
    if (CanImplicitlyCastTo(right_type, left_type) &&
        comparison.SupportsTypes(left_type, left_type)) {
      left_casted_type = right_casted_type = left_type;
      return &comparison;
    }
  }

  // There isn't a binary operation in the left input's type-system that. Let's
  // check the right input's type-system, but only if we can implicitly cast
  // the left input type to the right input type

  const auto &right_type_system = right_type.GetTypeSystem();
  for (const auto &comparison_info : right_type_system.comparison_table_) {
    // Can we use this comparison by implicitly casting the left input type to
    // the right input type?
    const auto &comparison = comparison_info.comparison;
    if (CanImplicitlyCastTo(left_type, right_type) &&
        comparison.SupportsTypes(right_type, right_type)) {
      left_casted_type = right_casted_type = right_type;
      return &comparison;
    }
  }

  // Error
  std::string msg =
      StringUtil::Format("No comparison rule between types: %s and %s",
                         TypeIdToString(left_type.type_id).c_str(),
                         TypeIdToString(right_type.type_id).c_str());

  throw Exception{msg};
}

const TypeSystem::UnaryOperator *TypeSystem::GetUnaryOperator(
    OperatorId op_id, const Type &input_type) {
  const auto &type_system = input_type.GetTypeSystem();
  for (const auto &unary_op_info : type_system.unary_op_table_) {
    // Is this the operation we want? If not, keep looking ...
    if (unary_op_info.op_id != op_id) {
      continue;
    }

    // Can we use the operation without any implicit casting?
    const auto &unary_operation = unary_op_info.unary_operation;
    if (unary_operation.SupportsType(input_type)) {
      return &unary_operation;
    }
  }

  // Error
  std::string msg = StringUtil::Format(
      "No compatible '%s' unary operator for input type: '%s'",
      TypeIdToString(input_type.type_id).c_str());

  throw Exception{msg};
}

const TypeSystem::BinaryOperator *TypeSystem::GetBinaryOperator(
    OperatorId op_id, const Type &left_type, Type &left_casted_type,
    const Type &right_type, Type &right_casted_type) {
  const auto &left_type_system = left_type.GetTypeSystem();
  for (const auto &binary_op_info : left_type_system.binary_op_table_) {
    // Is this the operation we want? If not, keep looking ...
    if (binary_op_info.op_id != op_id) {
      continue;
    }

    // Can we use the operation without any implicit casting?
    const auto &binary_operation = binary_op_info.binary_operation;
    if (binary_operation.SupportsTypes(left_type, right_type)) {
      left_casted_type = left_type;
      right_casted_type = right_type;
      return &binary_operation;
    }

    // Check if the right input type can be casted to the left input type
    if (CanImplicitlyCastTo(right_type, left_type) &&
        binary_operation.SupportsTypes(left_type, left_type)) {
      left_casted_type = right_casted_type = left_type;
      return &binary_operation;
    }
  }

  // There isn't a binary operation in the left input's type-system that. Let's
  // check the right input's type-system, but only if we can implicitly cast
  // the left input type to the right input type

  const auto &right_type_system = right_type.GetTypeSystem();
  for (const auto &binary_op_info : right_type_system.binary_op_table_) {
    // Is this the operation we want? If not, keep looking ...
    if (binary_op_info.op_id != op_id) {
      continue;
    }

    // Can we use the operation without any implicit casting?
    const auto &binary_operation = binary_op_info.binary_operation;
    if (CanImplicitlyCastTo(left_type, right_type) &&
        binary_operation.SupportsTypes(right_type, right_type)) {
      left_casted_type = right_type;
      right_casted_type = right_type;
      return &binary_operation;
    }
  }

  // Error
  std::string msg =
      StringUtil::Format("No compatible '%s' operator for input types: %s, %s",
                         OperatorIdToString(op_id).c_str(),
                         TypeIdToString(left_type.type_id).c_str(),
                         TypeIdToString(right_type.type_id).c_str());
  throw Exception{msg};
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
