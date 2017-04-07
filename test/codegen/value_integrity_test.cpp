//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_integrity_test.cpp
//
// Identification: test/codegen/value_integrity_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/type.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

class ValueIntegrityTest : public PelotonCodeGenTest {
 public:
  ValueIntegrityTest() : PelotonCodeGenTest() {}
};

// This test sets up a function taking a single argument. The body of the
// function divides a constant value with the value of the argument. After
// JITing the function, we test the division by passing in a zero value divisor.
template <typename CType>
void DivideByZeroTest(type::Type::TypeId data_type, ExpressionType op) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen{code_context};

  llvm::Type *llvm_type = nullptr, *llvm_len_type = nullptr;
  codegen::Type::GetTypeForMaterialization(codegen, data_type, llvm_type,
                                           llvm_len_type);

  codegen::FunctionBuilder function{
      code_context, "test", codegen.VoidType(), {{"arg", llvm_type}}};
  {
    codegen::Value a = codegen::Type::GetMaxValue(codegen, data_type);
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

    codegen.CallPrintf("%lu\n", {res.GetValue()});

    function.ReturnAndFinish();
  }

  // Should be able to compile
  EXPECT_TRUE(code_context.Compile());

  typedef void (*func)(CType);
  func f = (func)code_context.GetFunctionPointer(function.GetFunction());
  EXPECT_THROW(f(0), DivideByZeroException);
}

template <typename CType>
void OverflowTest(type::Type::TypeId data_type, ExpressionType op) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen{code_context};

  llvm::Type *llvm_type = nullptr, *llvm_len_type = nullptr;
  codegen::Type::GetTypeForMaterialization(codegen, data_type, llvm_type,
                                           llvm_len_type);

  codegen::FunctionBuilder function{
      code_context, "test", codegen.VoidType(), {{"arg", llvm_type}}};
  {
    codegen::Value initial{data_type, function.GetArgumentByPosition(0)};
    codegen::Value res;
    switch (op) {
      case ExpressionType::OPERATOR_PLUS: {
        // initial + MAX_FOR_TYPE
        codegen::Value next = codegen::Type::GetMaxValue(codegen, data_type);
        res = next.Add(codegen, initial);
        break;
      }
      case ExpressionType::OPERATOR_MINUS: {
        // a - MIN_FOR_TYPE
        codegen::Value next = codegen::Type::GetMinValue(codegen, data_type);
        res = initial.Sub(codegen, next);
        break;
      }
      case ExpressionType::OPERATOR_MULTIPLY: {
        // a * INT32_MAX
        codegen::Value next = codegen::Type::GetMaxValue(codegen, data_type);
        res = initial.Mul(codegen, next);
        break;
      }
      /*
      case ExpressionType::OPERATOR_DIVIDE: {
        // Overflow when: lhs == INT_MIN && rhs == -1
        // (a - 1) --> this makes a[0] = -1
        // INT64_MIN / (a - 1)
        auto *const_min_exp = CodegenTestUtils::ConstIntExpression(INT32_MIN);
        auto *const_one_exp = CodegenTestUtils::ConstIntExpression(1);
        auto *a_sub_one = new expression::OperatorExpression(
            ExpressionType::OPERATOR_MINUS, type::Type::TypeId::INTEGER,
            a_col_exp, const_one_exp);
        a_op_lim = new expression::OperatorExpression(
            exp_type, type::Type::TypeId::INTEGER, const_min_exp, a_sub_one);
        break;
      }
      */
      default: {
        throw Exception{"Invalid expression type for overflow check"};
      }
    }

    codegen.CallPrintf("%lu\n", {res.GetValue()});

    function.ReturnAndFinish();
  }

  // Should be able to compile
  EXPECT_TRUE(code_context.Compile());

  typedef void (*func)(CType);
  func f = (func)code_context.GetFunctionPointer(function.GetFunction());
  EXPECT_THROW(f(2), std::overflow_error);
}

TEST_F(ValueIntegrityTest, IntegerOverflow) {
  auto overflowable_ops = {ExpressionType::OPERATOR_MINUS,
                           ExpressionType::OPERATOR_PLUS,
                           ExpressionType::OPERATOR_MULTIPLY};
  for (auto op : overflowable_ops) {
    OverflowTest<int8_t>(type::Type::TypeId::TINYINT, op);
    OverflowTest<int16_t>(type::Type::TypeId::SMALLINT, op);
    OverflowTest<int32_t>(type::Type::TypeId::INTEGER, op);
    OverflowTest<int64_t>(type::Type::TypeId::BIGINT, op);
  }
}

TEST_F(ValueIntegrityTest, IntegerDivideByZero) {
  auto div0_ops = {ExpressionType::OPERATOR_DIVIDE,
                   ExpressionType::OPERATOR_MOD};
  for (auto op : div0_ops) {
    DivideByZeroTest<int8_t>(type::Type::TypeId::TINYINT, op);
    DivideByZeroTest<int16_t>(type::Type::TypeId::SMALLINT, op);
    DivideByZeroTest<int32_t>(type::Type::TypeId::INTEGER, op);
    DivideByZeroTest<int64_t>(type::Type::TypeId::BIGINT, op);
  }
}

}  // namespace test
}  // namespace peloton