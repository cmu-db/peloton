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

#include "codegen/testing_codegen_util.h"

#include <vector>

#include "codegen/type/array_type.h"
#include "codegen/type/type.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/date_type.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/smallint_type.h"
#include "codegen/type/timestamp_type.h"
#include "codegen/type/tinyint_type.h"
#include "codegen/type/varbinary_type.h"
#include "codegen/type/varchar_type.h"

namespace peloton {
namespace test {

class TypeIntegrityTest : public PelotonCodeGenTest {};

TEST_F(TypeIntegrityTest, ImplicitCastTest) {
  struct ImplicitCastTestCase {
    codegen::type::Type source_type;
    std::vector<codegen::type::Type> target_types;

    ImplicitCastTestCase(const codegen::type::Type &_source_type,
                         const std::vector<codegen::type::Type> &_target_types)
        : source_type(_source_type), target_types(_target_types) {}

    bool CanCastTo(const codegen::type::Type &t) const {
      return std::find(target_types.begin(), target_types.end(), t) !=
             target_types.end();
    }
  };

  // This list contains a list of the the above structs that essentially tracks
  // which SQL types can be implicitly casted to which other SQL types
  std::vector<ImplicitCastTestCase> implicit_casting_table = {
      // Boolean can only be casted to itself
      {codegen::type::Boolean::Instance(),
       {codegen::type::Boolean::Instance()}},

      //////////////////////////////////////////////////////////////////////////
      // All other integral types can only be casted to WIDER integral types
      //////////////////////////////////////////////////////////////////////////

      {codegen::type::TinyInt::Instance(),
       {codegen::type::TinyInt::Instance(), codegen::type::SmallInt::Instance(),
        codegen::type::Integer::Instance(), codegen::type::BigInt::Instance(),
        codegen::type::Decimal::Instance()}},

      {codegen::type::SmallInt::Instance(),
       {codegen::type::SmallInt::Instance(), codegen::type::Integer::Instance(),
        codegen::type::BigInt::Instance(), codegen::type::Decimal::Instance()}},

      {codegen::type::Integer::Instance(),
       {codegen::type::Integer::Instance(), codegen::type::BigInt::Instance(),
        codegen::type::Decimal::Instance()}},

      {codegen::type::BigInt::Instance(),
       {codegen::type::BigInt::Instance(), codegen::type::Decimal::Instance()}},

      {codegen::type::Decimal::Instance(),
       {codegen::type::Decimal::Instance()}},
  };

  const std::vector<codegen::type::Type> types_to_test = {
      codegen::type::Boolean::Instance(),  codegen::type::TinyInt::Instance(),
      codegen::type::SmallInt::Instance(), codegen::type::Integer::Instance(),
      codegen::type::BigInt::Instance(),   codegen::type::Decimal::Instance(),
      codegen::type::Date::Instance(),     codegen::type::Timestamp::Instance(),
      codegen::type::Varchar::Instance(),  codegen::type::Varbinary::Instance(),
      codegen::type::Array::Instance()};

  // Check that all the proper types can be casted when allowed
  for (const auto &test_case : implicit_casting_table) {
    const auto &src_type = test_case.source_type;
    for (const auto &target_type : types_to_test) {
      if (test_case.CanCastTo(target_type)) {
        EXPECT_TRUE(codegen::type::TypeSystem::CanImplicitlyCastTo(
            src_type, target_type));
      } else {
        EXPECT_FALSE(codegen::type::TypeSystem::CanImplicitlyCastTo(
            src_type, target_type));
      }
    }
  }
}

// This test performs checks that comparisons between every possible pair of
// (the most important) input types are possible through potentially implicitly
// casting the inputs.
TEST_F(TypeIntegrityTest, ComparisonWithImplicitCastTest) {
  const std::vector<codegen::type::Type> types_to_test = {
      codegen::type::Boolean::Instance(),
      codegen::type::TinyInt::Instance(),
      codegen::type::SmallInt::Instance(),
      codegen::type::Integer::Instance(),
      codegen::type::BigInt::Instance(),
      codegen::type::Decimal::Instance(),
      codegen::type::Date::Instance(),
      codegen::type::Timestamp::Instance()};

  for (const auto &left_type : types_to_test) {
    for (const auto &right_type : types_to_test) {
      const auto &type_system = left_type.GetTypeSystem();

      codegen::type::Type type_to_cast_left = left_type;
      codegen::type::Type type_to_cast_right = right_type;

      const codegen::type::TypeSystem::Comparison *result;
      if (codegen::type::TypeSystem::CanImplicitlyCastTo(left_type,
                                                         right_type) ||
          codegen::type::TypeSystem::CanImplicitlyCastTo(right_type,
                                                         left_type)) {
        // If the types are implicitly cast-able to one or the other, the
        // comparison shouldn't fail
        EXPECT_NO_THROW({
          result = type_system.GetComparison(left_type, type_to_cast_left,
                                             right_type, type_to_cast_right);
        });
        EXPECT_NE(nullptr, result);
      } else {
        // The types are **NOT** implicitly cast-able, this should fail
        EXPECT_THROW({
          result = type_system.GetComparison(left_type, type_to_cast_left,
                                             right_type, type_to_cast_right);
        }, Exception);
      }
    }
  }
}

// TODO: This test only does math ops. We need a generic way to test binary ops.
TEST_F(TypeIntegrityTest, MathOpWithImplicitCastTest) {
  const auto binary_ops = {OperatorId::Add, OperatorId::Sub, OperatorId::Mul,
                           OperatorId::Div, OperatorId::Mod};

  const std::vector<codegen::type::Type> types_to_test = {
      codegen::type::Boolean::Instance(),
      codegen::type::TinyInt::Instance(),
      codegen::type::SmallInt::Instance(),
      codegen::type::Integer::Instance(),
      codegen::type::BigInt::Instance(),
      codegen::type::Decimal::Instance(),
      codegen::type::Date::Instance(),
      codegen::type::Timestamp::Instance()};

  const auto IsNumber = [](const codegen::type::Type &type) {
    switch (type.type_id) {
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER:
      case peloton::type::TypeId::BIGINT:
      case peloton::type::TypeId::DECIMAL:
        return true;
      default:
        return false;
    }
  };

  for (auto left_type : types_to_test) {
    for (auto right_type : types_to_test) {
      for (auto bin_op : binary_ops) {
        const auto &type_system = left_type.GetTypeSystem();

        codegen::type::Type type_to_cast_left = left_type;
        codegen::type::Type type_to_cast_right = right_type;
        const codegen::type::TypeSystem::BinaryOperator *result;
        if (IsNumber(left_type) && IsNumber(right_type)) {
          // If the types are implicitly cast-able to one or the other, the
          // comparison shouldn't fail
          EXPECT_NO_THROW({
            result = type_system.GetBinaryOperator(
                bin_op, left_type, type_to_cast_left, right_type,
                type_to_cast_right);
          });
          EXPECT_NE(nullptr, result);
        } else {
          // The types are **NOT** implicitly cast-able, this should fail
          EXPECT_THROW({
            result = type_system.GetBinaryOperator(
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
