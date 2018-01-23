#include "udf/ast_nodes.h"
#include <iostream>  ////TODO(PP) Remove
#include "catalog/catalog.h"
#include "codegen/lang/if.h"
#include "codegen/type/type.h"

namespace peloton {
namespace udf {

// Codegen for NumberExprAST
peloton::codegen::Value NumberExprAST::Codegen(
    peloton::codegen::CodeGen &codegen,
    UNUSED_ATTRIBUTE peloton::codegen::FunctionBuilder &fb) {
  auto number_codegen_val = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(val));
  return number_codegen_val;
}

// Codegen for VariableExprAST
peloton::codegen::Value VariableExprAST::Codegen(
    UNUSED_ATTRIBUTE peloton::codegen::CodeGen &codegen,
    peloton::codegen::FunctionBuilder &fb) {
  llvm::Value *val = fb.GetArgumentByName(name);

  if (val) {
    return peloton::codegen::Value(
        peloton::codegen::type::Type(type::TypeId::DECIMAL, false), val);
  }
  return LogErrorV("Unknown variable name");
}

// Codegen for BinaryExprAST
peloton::codegen::Value BinaryExprAST::Codegen(
    peloton::codegen::CodeGen &codegen, peloton::codegen::FunctionBuilder &fb) {
  peloton::codegen::Value left = lhs->Codegen(codegen, fb);
  peloton::codegen::Value right = rhs->Codegen(codegen, fb);
  if (left.GetValue() == 0 || right.GetValue() == 0) {
    return peloton::codegen::Value();
  }

  switch (op) {
    case '+': {
      return left.Add(codegen, right);
    }
    case '-': {
      return left.Sub(codegen, right);
    }
    case '*': {
      return left.Mul(codegen, right);
    }
    case '/': {
      return left.Div(codegen, right);
    }
    case '<': {
      auto val = left.CompareLt(codegen, right);
      codegen::type::Type valType(peloton::type::TypeId::DECIMAL, false);
      return val.CastTo(codegen, valType);
    }
    case '>': {
      auto val = left.CompareGt(codegen, right);
      codegen::type::Type valType(peloton::type::TypeId::DECIMAL, false);
      return val.CastTo(codegen, valType);
    }
    default:
      return LogErrorV("invalid binary operator");
  }
}

// Codegen for CallExprAST
peloton::codegen::Value CallExprAST::Codegen(
    peloton::codegen::CodeGen &codegen, peloton::codegen::FunctionBuilder &fb) {
  // Check if present in the current code context
  // Else, check the catalog and get it
  auto *callee_func = fb.GetFunction();

  // TODO(PP) : Later change this to also check in the catalog
  if (callee_func == 0) {
    return LogErrorV("Unknown function referenced");
  }

  // If argument mismatch error.
  if (callee_func->arg_size() != args.size())
    return LogErrorV("Incorrect # arguments passed");

  std::vector<llvm::Value *> args_val;
  for (unsigned i = 0, size = args.size(); i != size; ++i) {
    args_val.push_back(args[i]->Codegen(codegen, fb).GetValue());
    if (args_val.back() == 0) {
      return LogErrorV("Arguments could not be passed in");
    }
  }

  auto *call_ret = codegen.CallFunc(callee_func, args_val);

  auto call_val = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false), call_ret);

  return call_val;
}

peloton::codegen::Value IfExprAST::Codegen(
    peloton::codegen::CodeGen &codegen, peloton::codegen::FunctionBuilder &fb) {
  auto compare_value = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
      codegen.ConstDouble(1.0));

  peloton::codegen::Value cond_expr_value = cond_expr->Codegen(codegen, fb);
  peloton::codegen::Value if_result;
  peloton::codegen::Value else_result;

  // Codegen If condition expression
  codegen::lang::If entry_cond{
      codegen, cond_expr_value.CompareEq(codegen, compare_value), "entry_cond"};
  {
    // Codegen the then statements
    if_result = then_stmt->Codegen(codegen, fb);
  }
  entry_cond.ElseBlock("multipleValue");
  {
    // codegen the else statements
    else_result = else_stmt->Codegen(codegen, fb);
  }
  entry_cond.EndIf();

  auto *val1 = if_result.GetValue();
  auto *val2 = else_result.GetValue();

  auto *final_result = entry_cond.BuildPHI(val1, val2);

  auto return_val = peloton::codegen::Value(
      peloton::codegen::type::Type(type::TypeId::DECIMAL, false), final_result);

  return return_val;
}

// Codegen for FunctionAST
llvm::Function *FunctionAST::Codegen(peloton::codegen::CodeGen &codegen,
                                     peloton::codegen::FunctionBuilder &fb) {
  peloton::codegen::Value ret = body->Codegen(codegen, fb);

  fb.ReturnAndFinish(ret.GetValue());

  return fb.GetFunction();
}

std::unique_ptr<ExprAST> LogError(UNUSED_ATTRIBUTE const char *str) {
  LOG_TRACE("Error: %s\n", str);
  return 0;
}

peloton::codegen::Value LogErrorV(const char *str) {
  LogError(str);
  return peloton::codegen::Value();
}

}  // namespace udf
}  // namespace peloton
