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
        auto *const_min_exp = ConstIntExpr(INT32_MIN).release();
        auto *const_one_exp = ConstIntExpr(1).release();
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

enum class Cmp : uint32_t { Eq, Ne, Lt, Lte, Gt, Gte };

enum class Op : uint32_t { Add, Sub, Mul, Div, Rem };

static const std::vector<Cmp> kCmpFuncs = {Cmp::Eq,  Cmp::Ne, Cmp::Lt,
                                           Cmp::Lte, Cmp::Gt, Cmp::Gte};

static const std::vector<Op> kOpFuncs = {Op::Add, Op::Sub, Op::Mul, Op::Div,
                                         Op::Rem};

static const std::vector<type::Type::TypeId> kTypesToTest = {
    type::Type::BOOLEAN, type::Type::TINYINT,  type::Type::SMALLINT,
    type::Type::INTEGER, type::Type::BIGINT,   type::Type::DECIMAL,
    type::Type::DATE,    type::Type::TIMESTAMP};

codegen::Value DoCmp(Cmp cmp, codegen::CodeGen &codegen,
                     const codegen::Value &left, const codegen::Value &right) {
  switch (cmp) {
    case Cmp::Eq:
      return left.CompareEq(codegen, right);
    case Cmp::Ne:
      return left.CompareNe(codegen, right);
    case Cmp::Lt:
      return left.CompareLt(codegen, right);
    case Cmp::Lte:
      return left.CompareLte(codegen, right);
    case Cmp::Gt:
      return left.CompareGt(codegen, right);
    case Cmp::Gte:
      return left.CompareGte(codegen, right);
  }
  throw Exception{"WTF? Not possible"};
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

TEST_F(ValueIntegrityTest, ImplicitCastTest) {
  type::Type::TypeId input_type;

  // Tinyint's can be implicitly casted to any higher-bit integer type
  input_type = type::Type::TINYINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::TINYINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::SMALLINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::DECIMAL));

  // Small's can be implicitly casted to any higher-bit integer type
  input_type = type::Type::SMALLINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::SMALLINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::DECIMAL));

  // Integer's can be implicitly casted to any higher-bit integer type
  input_type = type::Type::INTEGER;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::INTEGER));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::DECIMAL));

  // Integer's can be implicitly casted to any higher-bit integer type
  input_type = type::Type::BIGINT;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::BIGINT));
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::DECIMAL));

  // Decimal's can only be casted to itself
  input_type = type::Type::DECIMAL;
  EXPECT_TRUE(
      codegen::Type::CanImplicitlyCastTo(input_type, type::Type::DECIMAL));
}

// This test performs every possible comparison between every possible pair of
// (the most important) null and non-null input types. We use the 'control'
// variable to control left and right nullability: the first and second LSB in
// the control are used to determine if the right and left inputs are NULL,
// respectively.
TEST_F(ValueIntegrityTest, CompatibleComparisonWithImplicitCastTest) {
  codegen::CodeContext ctx;
  codegen::CodeGen codegen{ctx};

  for (uint8_t control = 0; control < 4; control++) {
    for (auto left_type : kTypesToTest) {
      for (auto right_type : kTypesToTest) {
        for (auto cmp : kCmpFuncs) {
          bool left_null = control & 0x02;
          bool right_null = control & 0x01;

          codegen::Value left =
              left_null ? codegen::Type::GetNullValue(codegen, left_type)
                        : codegen::Type::GetMinValue(codegen, left_type);
          codegen::Value right =
              right_null ? codegen::Type::GetNullValue(codegen, right_type)
                         : codegen::Type::GetMinValue(codegen, right_type);

          LOG_DEBUG("[%s,%s]: %s, %s, %u", TypeIdToString(left_type).c_str(),
                    TypeIdToString(right_type).c_str(),
                    (left_null ? "left-null" : "left-non-null"),
                    (right_null ? "right-null" : "right-non-null"),
                    static_cast<uint32_t>(cmp));

          codegen::Value result;

          if (codegen::Type::CanImplicitlyCastTo(left_type, right_type) ||
              codegen::Type::CanImplicitlyCastTo(right_type, left_type)) {
            // If the types are implicitly cast-able to one or the other, the
            // comparison shouldn't fail
            EXPECT_NO_THROW({ result = DoCmp(cmp, codegen, left, right); });

            if (control == 0) {
              EXPECT_FALSE(result.IsNullable());
            } else {
              EXPECT_TRUE(result.IsNullable());
            }
          } else {
            // The types are **NOT** implicitly cast-able, this should fail
            EXPECT_THROW({ result = DoCmp(cmp, codegen, left, right); },
                         Exception);
          }
        }
      }
    }
  }
}

}  // namespace test
}  // namespace peloton