//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_integrity_test.cpp
//
// Identification: test/codegen/value_integrity_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include <random>

#include "codegen/function_builder.h"
#include "codegen/type/tinyint_type.h"
#include "codegen/type/smallint_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/bigint_type.h"
#include "function/numeric_functions.h"

namespace peloton {
namespace test {

class ValueIntegrityTest : public PelotonCodeGenTest {};

// This test sets up a function taking a single argument. The body of the
// function divides a constant value with the value of the argument. After
// JITing the function, we test the division by passing in a zero value divisor.
template <typename CType>
void DivideByZeroTest(const codegen::type::Type &data_type, ExpressionType op) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen{code_context};

  const auto &sql_type = data_type.GetSqlType();

  llvm::Type *llvm_type = nullptr, *llvm_len_type = nullptr;
  sql_type.GetTypeForMaterialization(codegen, llvm_type, llvm_len_type);

  codegen::FunctionBuilder function{
      code_context, "test", codegen.VoidType(), {{"arg", llvm_type}}};
  {
    codegen::Value a = sql_type.GetMaxValue(codegen);
    codegen::Value divisor{data_type, function.GetArgumentByPosition(0)};
    codegen::Value res;
    switch (op) {
      case ExpressionType::OPERATOR_DIVIDE: {
        // a / 0
        res = a.Div(codegen, divisor);
        break;
      }
      case ExpressionType::OPERATOR_MOD: {
        // a % 0
        res = a.Mod(codegen, divisor);
        break;
      }
      default: {
        throw Exception{"Invalid expression type for divide-by-zero test"};
      }
    }

    codegen.Printf("%lu\n", {res.GetValue()});

    function.ReturnAndFinish();
  }

  // Should be able to compile
  EXPECT_TRUE(code_context.Compile());

  typedef void (*func)(CType);
  func f = (func)code_context.GetRawFunctionPointer(function.GetFunction());
  EXPECT_THROW(f(0), DivideByZeroException);
}

template <typename CType>
void OverflowTest(const codegen::type::Type &data_type, ExpressionType op) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen{code_context};

  const auto &sql_type = data_type.GetSqlType();

  llvm::Type *llvm_type = nullptr, *llvm_len_type = nullptr;
  sql_type.GetTypeForMaterialization(codegen, llvm_type, llvm_len_type);

  codegen::FunctionBuilder function{
      code_context, "test", codegen.VoidType(), {{"arg", llvm_type}}};
  {
    codegen::Value initial{data_type, function.GetArgumentByPosition(0)};
    codegen::Value res;
    switch (op) {
      case ExpressionType::OPERATOR_PLUS: {
        // initial + MAX_FOR_TYPE
        codegen::Value next = sql_type.GetMaxValue(codegen);
        res = next.Add(codegen, initial);
        break;
      }
      case ExpressionType::OPERATOR_MINUS: {
        // a - MIN_FOR_TYPE
        codegen::Value next = sql_type.GetMinValue(codegen);
        res = initial.Sub(codegen, next);
        break;
      }
      case ExpressionType::OPERATOR_MULTIPLY: {
        // a * INT32_MAX
        codegen::Value next = sql_type.GetMaxValue(codegen);
        res = initial.Mul(codegen, next);
        break;
      }
      /*
      case ExpressionType::OPERATOR_DIVIDE: {
        // Overflow when: lhs == INT_MIN && rhs == -1
        // (a - 1) --> this makes a[0] = -1
        // INT64_MIN / (a - 1)
        auto *const_min_exp = ConstIntExpr(INT32_MIN).release();
        auto *const_one_exp = ConstIntExpr(1).release();
        auto *a_sub_one = new expression::OperatorExpression(
            ExpressionType::OPERATOR_MINUS, type::TypeId::INTEGER,
            a_col_exp, const_one_exp);
        a_op_lim = new expression::OperatorExpression(
            exp_type, type::TypeId::INTEGER, const_min_exp, a_sub_one);
        break;
      }
      */
      default: {
        throw Exception{"Invalid expression type for overflow check"};
      }
    }

    codegen.Printf("%lu\n", {res.GetValue()});

    function.ReturnAndFinish();
  }

  // Should be able to compile
  EXPECT_TRUE(code_context.Compile());

  typedef void (*func)(CType);
  func f = (func)code_context.GetRawFunctionPointer(function.GetFunction());
  EXPECT_THROW(f(2), std::overflow_error);
}

TEST_F(ValueIntegrityTest, IntegerOverflow) {
  auto overflowable_ops = {ExpressionType::OPERATOR_MINUS,
                           ExpressionType::OPERATOR_PLUS,
                           ExpressionType::OPERATOR_MULTIPLY};
  for (auto op : overflowable_ops) {
    OverflowTest<int8_t>(codegen::type::TinyInt::Instance(), op);
    OverflowTest<int16_t>(codegen::type::SmallInt::Instance(), op);
    OverflowTest<int32_t>(codegen::type::Integer::Instance(), op);
    OverflowTest<int64_t>(codegen::type::BigInt::Instance(), op);
  }
}

TEST_F(ValueIntegrityTest, IntegerDivideByZero) {
  auto div0_ops = {ExpressionType::OPERATOR_DIVIDE,
                   ExpressionType::OPERATOR_MOD};
  for (auto op : div0_ops) {
    DivideByZeroTest<int8_t>(codegen::type::TinyInt::Instance(), op);
    DivideByZeroTest<int16_t>(codegen::type::SmallInt::Instance(), op);
    DivideByZeroTest<int32_t>(codegen::type::Integer::Instance(), op);
    DivideByZeroTest<int64_t>(codegen::type::BigInt::Instance(), op);
  }
}

namespace {

template <typename T>
using InputFunc = T (*)(const codegen::type::Type &, const char *, uint32_t);

template <typename T>
void TestInputIntegral(
    const codegen::type::Type &type, InputFunc<T> TestFunc,
    std::vector<std::pair<std::string, int64_t>> extra_valid_tests = {},
    std::vector<std::string> extra_invalid_tests = {},
    std::vector<std::string> extra_overflow_tests = {}) {
  // Default valid tests - these are valid for all integral types
  std::vector<std::pair<std::string, int64_t>> valid_tests = {{"0", 0},
                                                              {"-1", -1},
                                                              {"2", 2},
                                                              {"+3", 3},
                                                              {"  4", 4},
                                                              {"  -5", -5},
                                                              {"  +6", 6},
                                                              {"7  ", 7},
                                                              {"-8  ", -8},
                                                              {"  9  ", 9},
                                                              {"  -10  ", -10},
                                                              {"  +11  ", 11}};
  valid_tests.insert(valid_tests.end(), extra_valid_tests.begin(),
                     extra_valid_tests.end());

  // Default invalid tests
  std::vector<std::string> invalid_tests = {"a",       "-b",    "+c",   " 1c",
                                            "2d ",     "3 3",   "-4 4", "-5 a ",
                                            "  -6  a", "  c 7 "};
  invalid_tests.insert(invalid_tests.end(), extra_invalid_tests.begin(),
                       extra_invalid_tests.end());

  // Default overflow tests
  std::vector<std::string> overflow_tests = {
      std::to_string(std::numeric_limits<T>::min()) + "1",
      std::to_string(std::numeric_limits<T>::max()) + "1",
      "123456789123456789123456789"};
  overflow_tests.insert(overflow_tests.end(), extra_overflow_tests.begin(),
                        extra_overflow_tests.end());

  for (const auto &test : valid_tests) {
    auto *ptr = test.first.data();
    auto len = static_cast<uint32_t>(test.first.length());
    try {
      EXPECT_EQ(test.second, TestFunc(type, ptr, len));
    } catch (std::exception &e) {
      EXPECT_TRUE(false) << "Valid input '" << test.first << "' threw an error";
    }
  }

  for (const auto &test : invalid_tests) {
    auto *ptr = test.data();
    auto len = static_cast<uint32_t>(test.length());
    EXPECT_THROW(TestFunc(type, ptr, len), std::runtime_error)
        << "Input '" << test << "' was expected to throw an error, but did not";
  }

  for (const auto &test : overflow_tests) {
    auto *ptr = test.data();
    auto len = static_cast<uint32_t>(test.length());
    EXPECT_THROW(TestFunc(type, ptr, len), std::overflow_error)
        << "Input '" << test << "' expected to overflow, but did not";
  }
}
}  // namespace

TEST_F(ValueIntegrityTest, InputIntegralTypesTest) {
  codegen::type::Type tinyint{type::TypeId::TINYINT, false};
  TestInputIntegral<int8_t>(tinyint, function::NumericFunctions::InputTinyInt,
                            {{"-126", -126}, {"126", 126}});

  codegen::type::Type smallint{type::TypeId::SMALLINT, false};
  TestInputIntegral<int16_t>(smallint,
                             function::NumericFunctions::InputSmallInt);

  codegen::type::Type integer{type::TypeId::INTEGER, false};
  TestInputIntegral<int32_t>(integer, function::NumericFunctions::InputInteger);

  codegen::type::Type bigint{type::TypeId::BIGINT, false};
  TestInputIntegral<int64_t>(bigint, function::NumericFunctions::InputBigInt);
}

TEST_F(ValueIntegrityTest, InputDecimalTypesTest) {
  codegen::type::Type decimal{type::TypeId::DECIMAL, false};

  // First check some valid cases
  std::vector<std::pair<std::string, double>> valid_tests = {
      {"0.0", 0.0},
      {"-1.0", -1.0},
      {"2.0", 2.0},
      {"+3.0", 3.0},
      {"  4.0", 4.0},
      {"  -5.0", -5.0},
      {"  +6.0", 6.0},
      {"7.0  ", 7.0},
      {"-8.0  ", -8.0},
      {"  9.0  ", 9.0},
      {"  -10.0  ", -10.0},
      {"  +11.0  ", 11.0}};

  for (const auto &test_case : valid_tests) {
    auto *ptr = test_case.first.data();
    auto len = static_cast<uint32_t>(test_case.first.length());
    EXPECT_EQ(test_case.second,
              function::NumericFunctions::InputDecimal(decimal, ptr, len));
  }

  // Now let's try some invalid ones. Take each valid test and randomly insert
  // a character somewhere.
  std::vector<std::string> invalid_tests;

  std::random_device rd;
  std::mt19937 rng(rd());

  for (const auto &valid_test : valid_tests) {
    auto orig = valid_test.first;

    std::uniform_int_distribution<> dist(0, orig.length());
    auto pos = dist(rng);

    auto invalid_num = orig.substr(0, pos) + "aa" + orig.substr(pos);

    invalid_tests.push_back(invalid_num);
  }

  // Now check that each test throws an invalid string error
  for (const auto &invalid_test : invalid_tests) {
    auto *ptr = invalid_test.data();
    auto len = static_cast<uint32_t>(invalid_test.length());
    EXPECT_THROW(function::NumericFunctions::InputDecimal(decimal, ptr, len),
                 std::runtime_error)
        << "Input '" << invalid_test
        << "' expected to throw error, but passed parsing logic";
  }
}

}  // namespace test
}  // namespace peloton