//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_interpreter_test.cpp
//
// Identification: test/codegen/bytecode_interpreter_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/bytecode_interpreter.h"
#include "codegen/function_builder.h"
#include "codegen/interpreter/bytecode_builder.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "common/harness.h"

namespace peloton {
namespace test {

class BytecodeInterpreterTest : public PelotonTest {};

TEST_F(BytecodeInterpreterTest, PHIResolveTest) {
  // Create a loop that involves PHIs that have to be converted into move
  // instructions.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder main{
      code_context, "main", cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    auto *a = main.GetArgumentByPosition(0);
    auto *i = cg.Const32(0);

    codegen::lang::Loop loop{cg, cg.ConstBool(true), {{"i", i}, {"a", a}}};
    {
      llvm::Value *i = loop.GetLoopVar(0);
      llvm::Value *a = loop.GetLoopVar(1);

      a = cg->CreateSub(a, cg.Const32(1));
      i = cg->CreateAdd(i, cg.Const32(1));
      loop.LoopEnd(cg->CreateICmpULT(i, cg.Const32(10)), {i, a});
    }

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    auto *ret = final[1];
    main.ReturnAndFinish(ret);
  }

  // create Bytecode
  auto bytecode = codegen::interpreter::BytecodeBuilder::CreateBytecodeFunction(
      code_context, main.GetFunction());

  // run Bytecode
  codegen::interpreter::value_t arg = 44;
  codegen::interpreter::value_t ret =
      codegen::interpreter::BytecodeInterpreter::ExecuteFunction(bytecode,
                                                                 {arg});
  ASSERT_EQ(ret, arg - 10);
}

TEST_F(BytecodeInterpreterTest, PHISwapProblemTest) {
  // Produce the PHI swap problem, where additional moves have to be inserted
  // in order to retrieve the correct result.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder main{
      code_context, "main", cg.Int32Type(), {{"a", cg.Int32Type()}}};
  {
    auto *a = main.GetArgumentByPosition(0);
    auto *b = cg.Const32(0);
    auto *i = cg.Const32(0);

    codegen::lang::Loop loop{
        cg, cg.ConstBool(true), {{"i", i}, {"a", a}, {"b", b}}};
    {
      llvm::Value *i = loop.GetLoopVar(0);
      llvm::Value *a = loop.GetLoopVar(1);
      llvm::Value *b = loop.GetLoopVar(2);

      i = cg->CreateAdd(i, cg.Const32(1));
      loop.LoopEnd(cg->CreateICmpULT(i, cg.Const32(2)), {i, b, a});
    }

    std::vector<llvm::Value *> final;
    loop.CollectFinalLoopVariables(final);

    auto *ret = final[1];
    main.ReturnAndFinish(ret);
  }

  // create Bytecode
  auto bytecode = codegen::interpreter::BytecodeBuilder::CreateBytecodeFunction(
      code_context, main.GetFunction());

  // run Bytecode
  codegen::interpreter::value_t arg = 44;
  codegen::interpreter::value_t ret =
      codegen::interpreter::BytecodeInterpreter::ExecuteFunction(bytecode,
                                                                 {arg});
  ASSERT_EQ(ret, arg);
}

TEST_F(BytecodeInterpreterTest, OverflowIntrinsicsTest) {
  // Use the overflow intrinsics and retrieve their output. During bytecode
  // translation the extract instructions get omited and the values are written
  // directly to their destination value slot.

  // We call the intrinsics several times and check the result statically
  // right in the generated function. We merge all checks with AND and return
  // it to the test case at the end.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};
  codegen::FunctionBuilder main{code_context,
                                "main",
                                cg.Int32Type(),
                                {{"a", cg.Int32Type()}, {"b", cg.Int32Type()}}};
  {
    auto *a = main.GetArgumentByPosition(0);
    auto *b = main.GetArgumentByPosition(1);
    llvm::Value *add_overflow, *sub_overflow;
    llvm::Value *ret = cg.ConstBool(true);

    auto *add_result = cg.CallAddWithOverflow(a, b, add_overflow);
    auto *add_result_correct = cg->CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ,
                                              add_result, cg.Const32(10));
    ret = cg->CreateAnd(ret, add_result_correct);
    auto *add_overflow_correct = cg->CreateNot(add_overflow);
    ret = cg->CreateAnd(ret, add_overflow_correct);

    auto *sub_result =
        cg.CallSubWithOverflow(cg.Const32(2147483648), b, sub_overflow);
    auto *sub_result_correct = cg->CreateICmp(
        llvm::CmpInst::Predicate::ICMP_EQ, sub_result, cg.Const32(2147483642));
    ret = cg->CreateAnd(ret, sub_result_correct);
    ret = cg->CreateAnd(ret, sub_overflow);

    main.ReturnAndFinish(ret);
  }

  // create Bytecode
  auto bytecode = codegen::interpreter::BytecodeBuilder::CreateBytecodeFunction(
      code_context, main.GetFunction());

  // run Bytecode
  codegen::interpreter::value_t ret =
      codegen::interpreter::BytecodeInterpreter::ExecuteFunction(bytecode,
                                                                 {4, 6});
  ASSERT_EQ(ret, 1);
}

int f(int a, int b) { return a + b; }

TEST_F(BytecodeInterpreterTest, ExternalCallTest) {
  // Call an external function.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};

  // create LLVM function declaration
  auto *func_type = llvm::FunctionType::get(
      cg.Int32Type(), {cg.Int32Type(), cg.Int32Type()}, false);
  llvm::Function *func_decl =
      llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "f",
                             &(cg.GetCodeContext().GetModule()));
  code_context.RegisterExternalFunction(func_decl, (void *)f);

  codegen::FunctionBuilder main{code_context,
                                "main",
                                cg.Int32Type(),
                                {{"a", cg.Int32Type()}, {"b", cg.Int32Type()}}};
  {
    auto *a = main.GetArgumentByPosition(0);
    auto *b = main.GetArgumentByPosition(1);

    auto *ret = cg.CallFunc(func_decl, {a, b});

    main.ReturnAndFinish(ret);
  }

  // create Bytecode
  auto bytecode = codegen::interpreter::BytecodeBuilder::CreateBytecodeFunction(
      code_context, main.GetFunction());

  // run Bytecode
  codegen::interpreter::value_t ret =
      codegen::interpreter::BytecodeInterpreter::ExecuteFunction(bytecode,
                                                                 {4, 6});
  ASSERT_EQ(ret, 10);
}

TEST_F(BytecodeInterpreterTest, InternalCallTest) {
  // Call an internal function.

  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};

  codegen::FunctionBuilder f{code_context,
                             "f",
                             cg.Int32Type(),
                             {{"a", cg.Int32Type()}, {"b", cg.Int32Type()}}};
  {
    auto *a = f.GetArgumentByPosition(0);
    auto *b = f.GetArgumentByPosition(1);

    auto *ret = cg->CreateAdd(a, b);

    f.ReturnAndFinish(ret);
  }

  codegen::FunctionBuilder main{code_context,
                                "main",
                                cg.Int32Type(),
                                {{"a", cg.Int32Type()}, {"b", cg.Int32Type()}}};
  {
    auto *a = main.GetArgumentByPosition(0);
    auto *b = main.GetArgumentByPosition(1);

    auto *ret = cg.CallFunc(f.GetFunction(), {a, b});

    main.ReturnAndFinish(ret);
  }

  // create Bytecode
  auto bytecode = codegen::interpreter::BytecodeBuilder::CreateBytecodeFunction(
      code_context, main.GetFunction());

  // run Bytecode
  codegen::interpreter::value_t ret =
      codegen::interpreter::BytecodeInterpreter::ExecuteFunction(bytecode,
                                                                 {4, 6});
  ASSERT_EQ(ret, 10);
}

}  // namespace test
}  // namespace peloton