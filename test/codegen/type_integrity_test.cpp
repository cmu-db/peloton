//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_integrity_test.cpp
//
// Identification: ../peloton-1/test/codegen/type_integrity_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen_test_util.h"
#include "codegen/type.h"

namespace peloton {
namespace test {

class TypeIntegrityTest : public PelotonCodeGenTest {};

TEST_F(TypeIntegrityTest, ImplicitCastTest) {
  type::TypeId input_type;

  // Tinyint's can be implicitly casted to any higher-bit integer type
  input_type = type::TypeId::TINYINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::TINYINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::SMALLINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::DECIMAL));

  // Small's can be implicitly casted to any higher-bit integer type
  input_type = type::TypeId::SMALLINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::SMALLINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::DECIMAL));

  // Integer's can be implicitly casted to any higher-bit integer type
  input_type = type::TypeId::INTEGER;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::DECIMAL));

  // Integer's can be implicitly casted to any higher-bit integer type
  input_type = type::TypeId::BIGINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::DECIMAL));

  // Decimal's can only be casted to itself
  input_type = type::TypeId::DECIMAL;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::TypeId::DECIMAL));
}

// This test performs checks that comparisons between every possible pair of
// (the most important) input types are possible through potentially implicitly
// casting the inputs.
TEST_F(TypeIntegrityTest, ComparisonWithImplicitCastTest) {
  const auto types_to_test = {type::TypeId::BOOLEAN,  type::TypeId::TINYINT,
                              type::TypeId::SMALLINT, type::TypeId::INTEGER,
                              type::TypeId::BIGINT,   type::TypeId::DECIMAL,
                              type::TypeId::DATE,     type::TypeId::TIMESTAMP};

  for (auto left_type : types_to_test) {
    for (auto right_type : types_to_test) {
      type::TypeId type_to_cast_left, type_to_cast_right;
      const codegen::Type::Comparison *result;
      if (codegen::Type::CanImplicitlyCastTo(left_type, right_type) ||
          codegen::Type::CanImplicitlyCastTo(right_type, left_type)) {
        // If the types are implicitly cast-able to one or the other, the
        // comparison shouldn't fail
        EXPECT_NO_THROW({
          result = codegen::Type::GetComparison(left_type, type_to_cast_left,
                                                right_type, type_to_cast_right);
        });
        EXPECT_NE(nullptr, result);
      } else {
        // The types are **NOT** implicitly cast-able, this should fail
        EXPECT_THROW({
          result = codegen::Type::GetComparison(left_type, type_to_cast_left,
                                                right_type, type_to_cast_right);
        }, Exception);
      }
    }
  }
}

// TODO: This test only does math ops. We need a generic way to test binary ops.
TEST_F(TypeIntegrityTest, MathOpWithImplicitCastTest) {
  const auto binary_ops = {
      codegen::Type::OperatorId::Add, codegen::Type::OperatorId::Sub,
      codegen::Type::OperatorId::Mul, codegen::Type::OperatorId::Div,
      codegen::Type::OperatorId::Mod};

  const auto types_to_test = {type::TypeId::BOOLEAN,  type::TypeId::TINYINT,
                              type::TypeId::SMALLINT, type::TypeId::INTEGER,
                              type::TypeId::BIGINT,   type::TypeId::DECIMAL,
                              type::TypeId::DATE,     type::TypeId::TIMESTAMP};

  const auto IsNumber = [](type::TypeId t) {
    return codegen::Type::IsIntegral(t) || codegen::Type::IsNumeric(t);
  };

  for (auto left_type : types_to_test) {
    for (auto right_type : types_to_test) {
      for (auto bin_op : binary_ops) {
        type::TypeId type_to_cast_left, type_to_cast_right;
        const codegen::Type::BinaryOperator *result;
        if (IsNumber(left_type) && IsNumber(right_type)) {
          // If the types are implicitly cast-able to one or the other, the
          // comparison shouldn't fail
          EXPECT_NO_THROW({
            result = codegen::Type::GetBinaryOperator(
                bin_op, left_type, type_to_cast_left, right_type,
                type_to_cast_right);
          });
          EXPECT_NE(nullptr, result);
        } else {
          // The types are **NOT** implicitly cast-able, this should fail
          EXPECT_THROW({
            result = codegen::Type::GetBinaryOperator(
                bin_op, left_type, type_to_cast_left, right_type,
                type_to_cast_right);
          }, Exception);
        }
      }
    }
  }
}

}  // namespace test
}  // namespace peloton
