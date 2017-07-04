//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder_test.cpp
//
// Identification: test/codegen/function_builder_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "include/codegen/proxy/runtime_functions_proxy.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class FunctionBuilderTest : public PelotonTest {};

TEST_F(FunctionBuilderTest, ConstructSingleFunction) {
  // Generate a function like so:
  // define @test() {
  //   ret i32 44;
  // }

  uint32_t magic_num = 44;

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder func{code_context, "test", cg.Int32Type(), {}};
  {
    func.ReturnAndFinish(cg.Const32(magic_num));
  }

  ASSERT_TRUE(code_context.Compile());

  typedef int (*func_t)(void);
  func_t fn = (func_t)code_context.GetFunctionPointer(func.GetFunction());
  ASSERT_EQ(fn(), magic_num);
}

TEST_F(FunctionBuilderTest, ConstructNestedFunction) {
  // In this test, we want to construct the following scenario:
  // define void @test(i32 %a) {
  //   %tmp = mul i32 %a, i32 44
  //   ret i32 %tmp;
  // }
  //
  // define i32 @main() {
  //  %x = call i32 test()
  //  ret i32 %x
  // }
  //
  // We want to construct @test **during** the construction of @main. We're
  // testing that this nesting is able to restore appropriate insertion points
  // in the IRBuilder.

  uint32_t magic_num = 44;

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder main{
      code_context, "main", cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    codegen::FunctionBuilder test{
        code_context, "test", cg.Int32Type(), {{"a", cg.Int32Type()}}};
    {
      auto *arg_a = test.GetArgumentByPosition(0);
      auto *ret = cg->CreateMul(arg_a, cg.Const32(magic_num));
      test.ReturnAndFinish(ret);
    }

    // Now call the @test function that was we just constructed, then return 44
    auto *main_arg = main.GetArgumentByPosition(0);
    auto *ret = cg.CallFunc(code_context.GetFunction("test"), {main_arg});
    main.ReturnAndFinish(ret);
  }

  // Make sure we can compile everything
  ASSERT_TRUE(code_context.Compile());

  typedef int (*func_t)(uint32_t);
  func_t fn = (func_t)code_context.GetFunctionPointer(main.GetFunction());
  ASSERT_EQ(fn(1), 44);
}

}  // namespace test
}  // namespace peloton