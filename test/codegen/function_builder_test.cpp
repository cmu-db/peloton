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
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class FunctionHelperTest : public PelotonTest {};

TEST_F(FunctionHelperTest, ConstructSingleFunction) {

  // Generate a function like so:
  // define @Test(i32 a) {
  //   printf("Hello, world!\n");
  //   return 44;
  // }

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder func{code_context, "Test", cg.Int32Type(), {}};
  {
    cg.CallPrintf("Hello, world!\n", {});
    func.ReturnAndFinish(cg.Const32(44));
  }

  ASSERT_TRUE(code_context.Compile());

  typedef int (*func_t)(void);
  func_t fn = (func_t)code_context.GetFunctionPointer(func.GetFunction());
  ASSERT_EQ(fn(), 44);
}

TEST_F(FunctionHelperTest, ConstructNestedFunction) {
  // In this test, we want to construct the following scenario:
  // define void @test() {
  //   printf("Hello, world!\n")
  // }
  //
  // define i32 @main() {
  //  test()
  //  ret 44
  // }
  //
  // We want to construct @test (((during))) the construction of main. We're
  // testing that this nesting is able to restore appropriate insertion points
  // in the IRBuilder.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder main{code_context, "main", cg.Int32Type(), {}};
  {
    codegen::FunctionBuilder test{code_context, "test", cg.VoidType(), {}};
    {
      cg.CallPrintf("Hello, world!\n", {});
      test.ReturnAndFinish();
    }

    // Now call the @test function that was we just constructed, then return 44
    cg.CallFunc(code_context.GetFunction("test"), {});
    main.ReturnAndFinish(cg.Const32(44));
  }

  // Make sure we can compile everything
  ASSERT_TRUE(code_context.Compile());

  typedef int (*func_t)(void);
  func_t fn = (func_t)code_context.GetFunctionPointer(main.GetFunction());
  ASSERT_EQ(fn(), 44);
}

}  // namespace test
}  // namespace peloton