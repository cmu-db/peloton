//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if_test.cpp
//
// Identification: test/codegen/loop_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// This file contains some code gen tests with loops.

#include "common/harness.h"

#include "codegen/function_builder.h"
#include "codegen/if.h"
#include "codegen/loop.h"
#include "codegen/runtime_functions_proxy.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

class LoopTest : public PelotonTest {};

TEST_F(LoopTest, SimpleLoop) {
  const std::string func_name = "TestSimpleLoop";
  codegen::CodeContext code_context;
  codegen::CodeGen cg(code_context);

  /*
   * Generate a function like so:
   * define i32 @TestSimpleLoop(i32 a) {
   *   i32 x := 1;
   *   for (i32 i := 0; i < a; i++) {
   *     x := x * 2;
   *   }
   *   return x;
   * }
   *
   * Which, with our loop interface, looks like this:
   * define i32 @TestSimpleLoop(i32 a) {
   *   init { i32 i := 0, i32 x := 1 } if (0 < a) {
   *     do {
   *       i32 x' := x * 2;
   *       i32 i' := i + 1;
   *     } while (i' < a) merge { i <- i', x <- x' }
   *   }
   *   return x;
   * }
   */

  codegen::FunctionBuilder func(code_context, func_name, cg.Int32Type(),
                                {{"a", cg.Int32Type()}});

  {
    llvm::Value *a = func.GetArgumentByName("a");

    codegen::Loop loop(cg, cg->CreateICmpSLT(cg.Const32(0), a),
                       {{"i", cg.Const32(0)}, {"x", cg.Const32(1)}});

    // Loop Begins.
    llvm::Value *i = loop.GetLoopVar(0);
    llvm::Value *x = loop.GetLoopVar(1);

    llvm::Value *x2 = cg->CreateMul(x, cg.Const32(2));
    llvm::Value *i2 = cg->CreateAdd(i, cg.Const32(1));

    // Loop Ends.
    loop.LoopEnd(cg->CreateICmpSLT(i2, a), {i2, x2});

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    func.ReturnAndFinish(final[1]);
  }

  ASSERT_TRUE(code_context.Compile());

  std::function<int(int)> f =
      (int (*)(int))(code_context.GetFunctionPointer(func.GetFunction()));

  ASSERT_EQ(1, f(0));
  ASSERT_EQ(2, f(1));
  ASSERT_EQ(4, f(2));
}

// Generate a fibonacci function and run it.
TEST_F(LoopTest, Fibonacci) {
  /*
   * Generate a function like so:
   * define i32 @Fibonacci(i32 n) {
   *   i32 prev := 0;
   *   i32 curr := 1;
   *   for (i32 i = 1; i <= n; i++) {
   *     i32 next := prev + curr;
   *     prev := curr;
   *     curr := next;
   *   }
   *   return curr;
   * }
   *
   * Which, with our loop interface, looks like this:
   * define i32 @Fibonacci(i32 n) {
   *   init { i32 i := 1, i32 prev := 0, i32 curr := 1 } if (0 < n) {
   *     do {
   *       i32 next := prev + curr;
   *       i32 i' := i + 1;
   *     } while (i' <= n) merge { i <- i', prev <- curr, curr <- next }
   *   }
   *   return curr;
   * }
   */

  const std::string func_name = "Fibonacci";
  codegen::CodeContext code_context;
  codegen::CodeGen cg(code_context);

  codegen::FunctionBuilder func(code_context, func_name, cg.Int32Type(),
                                {{"n", cg.Int32Type()}});

  {  // Function begins.

    llvm::Value *n = func.GetArgumentByName("n");

    codegen::Loop loop(cg, cg->CreateICmpSLT(cg.Const32(1), n),
                       {{"i", cg.Const32(1)},
                        {"prev", cg.Const32(0)},
                        {"curr", cg.Const32(1)}});

    {  // Loop begins.

      llvm::Value *i = loop.GetLoopVar(0);
      llvm::Value *prev = loop.GetLoopVar(1);
      llvm::Value *curr = loop.GetLoopVar(2);

      llvm::Value *next = cg->CreateAdd(prev, curr, "next");
      llvm::Value *i_ = cg->CreateAdd(i, cg.Const32(1), "i_");

      loop.LoopEnd(cg->CreateICmpSLE(i_, n), {i_, curr, next});
    }  // Loop ends.

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    func.ReturnAndFinish(final[2]);

  }  // Function ends.

  ASSERT_TRUE(code_context.Compile());

  std::function<int(int)> f =
      (int (*)(int))(code_context.GetFunctionPointer(func.GetFunction()));

  ASSERT_EQ(1, f(0));
  ASSERT_EQ(1, f(1));
  ASSERT_EQ(2, f(2));
  ASSERT_EQ(3, f(3));
  ASSERT_EQ(5, f(4));
  ASSERT_EQ(8, f(5));
}

}  // namespace test
}  // namespace peloton
