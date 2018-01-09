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
#include "codegen/type/boolean_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/value.h"
#include "common/exception.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace type {

namespace {

Value GenerateBinaryHandleNull(
    CodeGen &codegen, const SqlType &result_type, const Value &left,
    const Value &right,
    const std::function<Value(CodeGen &codegen, const Value &left,
                              const Value &right)> &impl) {
  if (!left.IsNullable() && !right.IsNullable()) {
    // Neither input is NULLable, elide the NULL check
    return impl(codegen, left, right);
  }

  // One of the inputs is nullable, compute the null bit first
  auto *null = codegen->CreateOr(left.IsNull(codegen), right.IsNull(codegen));

  Value null_val, ret_val;
  lang::If is_null{codegen, null, "is_null"};
  {
    // If either value is null, the result of the operator is null
    null_val = result_type.GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If both values are not null, perform the non-null-aware operation
    ret_val = impl(codegen, left, right);
  }
  is_null.EndIf();
  return is_null.BuildPHI(null_val, ret_val);
}

}  // anonymous namespace

//===----------------------------------------------------------------------===//
//
// CastHandleNull
//
//===----------------------------------------------------------------------===//

Value TypeSystem::CastHandleNull::Eval(CodeGen &codegen, const Value &value,
                                       const Type &to_type) const {
  if (!value.IsNullable()) {
    // If the value isn't NULLable, avoid the NULL check and just invoke
    return Impl(codegen, value, to_type);
  }

  // The value is NULLable, we need to perform a null check

  Value null_val, ret_val;
  lang::If is_null{codegen, value.IsNull(codegen), "is_null"};
  {
    // If the value is NULL, return the NULL type for the target type
    null_val = to_type.GetSqlType().GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If both values are not null, perform the non-null-aware operation
    ret_val = Impl(codegen, value, to_type);
  }
  is_null.EndIf();

  return is_null.BuildPHI(null_val, ret_val);
}

//===----------------------------------------------------------------------===//
//
// SimpleComparisonHandleNull
//
//===----------------------------------------------------------------------===//

#define GEN_COMPARE(IMPL)                                                  \
  if (!left.IsNullable() && !right.IsNullable()) {                         \
    /* Neither left nor right are NULLable, elide the NULL check */        \
    return (IMPL);                                                         \
  }                                                                        \
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
  Value result = (IMPL);                                                   \
  /* Return the result with the computed null-bit */                       \
  return Value{result.GetType().AsNullable(), result.GetValue(), nullptr, null};

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareLt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareLtImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareLte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareLteImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareEq(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareEqImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareNe(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareNeImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareGt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareGtImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareGte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareGteImpl(codegen, left, right));
}

Value TypeSystem::SimpleComparisonHandleNull::EvalCompareForSort(
    CodeGen &codegen, const Value &left, const Value &right) const {
  GEN_COMPARE(CompareForSortImpl(codegen, left, right));
}

#undef GEN_COMPARE

//===----------------------------------------------------------------------===//
//
// ExpensiveComparisonHandleNull
//
//===----------------------------------------------------------------------===//

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareLt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareLtImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareLte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareLteImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareEq(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareEqImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareNe(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareNeImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareGt(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareGtImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareGte(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareGteImpl(codegen, left, right);
  };
  const auto &result_type = Boolean::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

Value TypeSystem::ExpensiveComparisonHandleNull::EvalCompareForSort(
    CodeGen &codegen, const Value &left, const Value &right) const {
  auto impl = [this](CodeGen &codegen, const Value &left, const Value &right) {
    return CompareForSortImpl(codegen, left, right);
  };
  const auto &result_type = Integer::Instance();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

//===----------------------------------------------------------------------===//
//
// UnaryOperatorHandleNull
//
//===----------------------------------------------------------------------===//
Value TypeSystem::UnaryOperatorHandleNull::Eval(
    CodeGen &codegen, const Value &val, const InvocationContext &ctx) const {
  if (!val.IsNullable()) {
    // If the input is not NULLable, elide the NULL check
    return Impl(codegen, val, ctx);
  }

  Value null_val, ret_val;
  lang::If is_null{codegen, val.IsNull(codegen), "is_null"};
  {
    // If the value is NULL, return the NULL value for the result type
    null_val = ResultType(val.GetType()).GetSqlType().GetNullValue(codegen);
  }
  is_null.ElseBlock();
  {
    // If the input isn't NULL, perform the non-null-aware operation
    ret_val = Impl(codegen, val, ctx);
  }
  is_null.EndIf();

  return is_null.BuildPHI(null_val, ret_val);
}

//===----------------------------------------------------------------------===//
//
// BinaryOperatorHandleNull
//
//===----------------------------------------------------------------------===//
Value TypeSystem::BinaryOperatorHandleNull::Eval(
    CodeGen &codegen, const Value &left, const Value &right,
    const InvocationContext &ctx) const {
  auto impl =
      [this, &ctx](CodeGen &codegen, const Value &left, const Value &right) {
        return Impl(codegen, left, right, ctx);
      };

  auto &result_type = ResultType(left.GetType(), right.GetType()).GetSqlType();
  return GenerateBinaryHandleNull(codegen, result_type, left, right, impl);
}

//===----------------------------------------------------------------------===//
//
// TypeSystem
//
//===----------------------------------------------------------------------===//
TypeSystem::TypeSystem(
    const std::vector<peloton::type::TypeId> &implicit_cast_table,
    const std::vector<TypeSystem::CastInfo> &explicit_cast_table,
    const std::vector<TypeSystem::ComparisonInfo> &comparison_table,
    const std::vector<TypeSystem::UnaryOpInfo> &unary_op_table,
    const std::vector<TypeSystem::BinaryOpInfo> &binary_op_table,
    const std::vector<TypeSystem::NaryOpInfo> &nary_op_table,
    const std::vector<TypeSystem::NoArgOpInfo> &no_arg_op_table)
    : implicit_cast_table_(implicit_cast_table),
      explicit_cast_table_(explicit_cast_table),
      comparison_table_(comparison_table),
      unary_op_table_(unary_op_table),
      binary_op_table_(binary_op_table),
      nary_op_table_(nary_op_table),
      no_arg_op_table_(no_arg_op_table) {}

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

const TypeSystem::NaryOperator *TypeSystem::GetNaryOperator(
    OperatorId op_id, const std::vector<Type> &arg_types) {
  for (const auto &arg_type : arg_types) {
    auto &type_system = arg_type.GetTypeSystem();

    for (const auto &nary_op_info : type_system.nary_op_table_) {
      // Is this the operation we want? If not, keep looking ...
      if (nary_op_info.op_id != op_id) {
        continue;
      }

      // Can we use the operation without any implicit casting?
      const auto &nary_operation = nary_op_info.nary_operation;
      if (nary_operation.SupportsTypes(arg_types)) {
        return &nary_operation;
      }
    }
  }

  std::string arg_types_str;
  for (size_t i = 0; i < arg_types.size(); i++) {
    if (i != 0) arg_types_str.append(",");
    arg_types_str.append(TypeIdToString(arg_types[i].type_id));
  }

  // Error
  throw Exception{StringUtil::Format(
      "No compatible '%s' operator for input types: %s",
      OperatorIdToString(op_id).c_str(), arg_types_str.c_str())};
}

}  // namespace type
}  // namespace codegen
}  // namespace peloton
