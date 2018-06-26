//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if_test.cpp
//
// Identification: test/codegen/if_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/type/integer_type.h"

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class IfTest : public PelotonTest {};

TEST_F(IfTest, TestIfOnly) {
  const std::string func_name = "TestIfOnly";

  // Generate a function like so:
  // define @TestIfnly(i32 a) {
  //   if (a < 10) {
  //     return 1
  //   } else {
  //     return 0;
  //   }
  // }

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder func{
      code_context, func_name, cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    llvm::Value *param_a = func.GetArgumentByName("a");

    codegen::Value va, vb;
    codegen::lang::If cond{cg, cg->CreateICmpSLT(param_a, cg.Const32(10))};
    {
      // a < 10
      va = {codegen::type::Integer::Instance(), cg.Const32(1)};
    }
    cond.ElseBlock();
    {
      // a >= 10
      vb = {codegen::type::Integer::Instance(), cg.Const32(0)};
    }
    cond.EndIf();
    func.ReturnAndFinish(cond.BuildPHI(va, vb).GetValue());
  }

  code_context.Compile();

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());

  ASSERT_TRUE(f != nullptr);

  EXPECT_EQ(f(9), 1);
  EXPECT_EQ(f(0), 1);
  EXPECT_EQ(f(-1), 1);
  EXPECT_EQ(f(10), 0);
  EXPECT_EQ(f(2000), 0);
}

TEST_F(IfTest, TestIfInsideLoop) {
  const std::string func_name = "TestIfInsideLoop";
  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};

  // Generate a function that counts the even numbers in the range [0,a) for
  // some parameter a:
  //
  // define i32 @TestIfInsideLoop(i32 a) {
  //   x = 0
  //   for (i32 i = 0; i < a; i++) {
  //     if (i % 2 == 0) {
  //       x++
  //     }
  //   }
  //   return x
  // }
  //
  // We call f(10) and so five "Hello"'s should print out. It's a pretty shitty
  // test, but it's a quick check to see if BB insertions work.

  codegen::FunctionBuilder func{
      code_context, func_name, cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    llvm::Value *param_a = func.GetArgumentByName("a");
    codegen::lang::Loop loop{cg,
                             cg->CreateICmpULT(cg.Const32(0), param_a),
                             {{"i", cg.Const32(0)}, {"x", cg.Const32(0)}}};
    {
      llvm::Value *i = loop.GetLoopVar(0);
      llvm::Value *x = loop.GetLoopVar(1);

      auto *divisible_by_two = cg->CreateURem(i, cg.Const32(2));

      llvm::Value *new_x = nullptr;
      codegen::lang::If pred{cg,
                             cg->CreateICmpEQ(cg.Const32(0), divisible_by_two)};
      {
        // i is divisible by 2
        new_x = cg->CreateAdd(x, cg.Const32(1));
      }
      pred.EndIf();
      x = pred.BuildPHI(new_x, x);

      i = cg->CreateAdd(i, cg.Const32(1));
      loop.LoopEnd(cg->CreateICmpULT(i, param_a), {i, x});
    }

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    func.ReturnAndFinish(final[1]);
  }

  code_context.Compile();

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());
  ASSERT_EQ(5, f(10));
}

TEST_F(IfTest, BreakTest) {
  const std::string func_name = "TestBreakLoop";
  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};

  // Genereate a funciton like so:
  // define @TestBreakLoop(i32 a) {
  //   for (i32 i = 0; i < a; i++) {
  //      if (i == 5) {
  //         break;
  //      }
  //   }
  //   return i;
  //
  //
  codegen::FunctionBuilder func{
      code_context, func_name, cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    llvm::Value *param_a = func.GetArgumentByName("a");
    codegen::lang::Loop loop{
        cg, cg->CreateICmpSLT(cg.Const32(0), param_a), {{"i", cg.Const32(0)}}};
    {
      llvm::Value *i = loop.GetLoopVar(0);

      codegen::lang::If pred{cg, cg->CreateICmpEQ(i, cg.Const32(5))};
      { loop.Break(); }
      pred.EndIf();

      i = cg->CreateAdd(i, cg.Const32(1));
      loop.LoopEnd(cg->CreateICmpSLT(i, param_a), {i});
    }

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    func.ReturnAndFinish(final[0]);
  }

  code_context.Compile();

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());
  ASSERT_EQ(0, f(-1));
  ASSERT_EQ(3, f(3));
  ASSERT_EQ(5, f(6));
  ASSERT_EQ(5, f(7));
}

TEST_F(IfTest, ComplexNestedIf) {
  const std::string func_name = "TestIfOnly";

  // Generate a function like so:
  // define @TestNestedIf(i32 a) {
  //   if (a < 10) {
  //     if (a < 5) {
  //       return -1;
  //     } else {
  //       return 0;
  //     }
  //   } else {
  //     return 1;
  //   }
  // }

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder func{
      code_context, func_name, cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    llvm::Value *param_a = func.GetArgumentByName("a");

    codegen::Value vab, vc;
    codegen::lang::If cond{cg, cg->CreateICmpSLT(param_a, cg.Const32(10))};
    {
      // a < 10
      codegen::Value va, vb;
      codegen::lang::If cond2{cg, cg->CreateICmpSLT(param_a, cg.Const32(5))};
      {
        // a < 5
        va = {codegen::type::Integer::Instance(), cg.Const32(-1)};
      }
      cond2.ElseBlock();
      {
        // a >= 5
        vb = {codegen::type::Integer::Instance(), cg.Const32(0)};
      }
      cond2.EndIf();
      vab = cond2.BuildPHI(va, vb);
    }
    cond.ElseBlock();
    {
      // a >= 10
      vc = {codegen::type::Integer::Instance(), cg.Const32(1)};
    }
    cond.EndIf();
    func.ReturnAndFinish(cond.BuildPHI(vab, vc).GetValue());
  }

  code_context.Compile();

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());

  ASSERT_TRUE(f != nullptr);

  EXPECT_EQ(f(1), -1);
  EXPECT_EQ(f(5), 0);
  EXPECT_EQ(f(-100), -1);

  EXPECT_EQ(f(6), 0);
  EXPECT_EQ(f(9), 0);

  EXPECT_EQ(f(10), 1);
  EXPECT_EQ(f(2000), 1);
}

}  // namespace test
}  // namespace peloton