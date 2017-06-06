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
#include "include/codegen/utils/if.h"
#include "include/codegen/utils/loop.h"
#include "codegen/runtime_functions_proxy.h"

#include "codegen/codegen_test_util.h"

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

    codegen::Value va(type::Type::TypeId::INTEGER);
    codegen::Value vb(type::Type::TypeId::INTEGER);
    codegen::If cond{cg, cg->CreateICmpSLT(param_a, cg.Const32(10))};
    {
      va = {type::Type::TypeId::INTEGER, cg.Const32(1)};
    }
    cond.ElseBlock();
    {
      vb = {type::Type::TypeId::INTEGER, cg.Const32(0)};
    }
    cond.EndIf();
    func.ReturnAndFinish(cond.BuildPHI(va, vb).GetValue());
  }

  ASSERT_TRUE(code_context.Compile());

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetFunctionPointer(func.GetFunction());

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
    codegen::Loop loop{cg,
                       cg->CreateICmpULT(cg.Const32(0), param_a),
                       {{"i", cg.Const32(0)}, {"x", cg.Const32(0)}}};
    {
      llvm::Value *i = loop.GetLoopVar(0);
      llvm::Value *x = loop.GetLoopVar(1);

      auto *divisible_by_two = cg->CreateURem(i, cg.Const32(2));

      llvm::Value *new_x = nullptr;
      codegen::If pred{cg, cg->CreateICmpEQ(cg.Const32(0), divisible_by_two)};
      {
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

  ASSERT_TRUE(code_context.Compile());

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetFunctionPointer(func.GetFunction());
  ASSERT_EQ(5, f(10));
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

    codegen::Value vab(type::Type::TypeId::INTEGER);
    codegen::Value vc(type::Type::TypeId::INTEGER);
    codegen::If cond{cg, cg->CreateICmpSLT(param_a, cg.Const32(10))};
    {
      codegen::Value va(type::Type::TypeId::INTEGER);
      codegen::Value vb(type::Type::TypeId::INTEGER);
      codegen::If cond2{cg, cg->CreateICmpSLT(param_a, cg.Const32(5))};
      { va = {type::Type::TypeId::INTEGER, cg.Const32(-1)}; }
      cond2.ElseBlock();
      { vb = {type::Type::TypeId::INTEGER, cg.Const32(0)}; }
      cond2.EndIf();
      vab = cond2.BuildPHI(va, vb);
    }
    cond.ElseBlock();
    { vc = {type::Type::TypeId::INTEGER, cg.Const32(1)}; }
    cond.EndIf();
    func.ReturnAndFinish(cond.BuildPHI(vab, vc).GetValue());
  }

  ASSERT_TRUE(code_context.Compile());

  typedef int (*ftype)(int);

  ftype f = (ftype)code_context.GetFunctionPointer(func.GetFunction());

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