#include "codegen/code_context.h"
#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "codegen/value.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/raw_ostream.h"  // For errs()
#include "type/type.h"

namespace peloton {
namespace udf {

using arg_type = type::TypeId;

// ExprAST - Base class for all expression nodes.
class ExprAST {
 public:
  virtual ~ExprAST() = default;

  virtual peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) = 0;
};

// NumberExprAST - Expression class for numeric literals like "1.0".
class NumberExprAST : public ExprAST {
  int val;

 public:
  NumberExprAST(int val) : val(val) {}

  peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) override;
};

// VariableExprAST - Expression class for referencing a variable, like "a".
class VariableExprAST : public ExprAST {
  std::string name;

 public:
  VariableExprAST(const std::string &name) : name(name) {}

  peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) override;
};

// BinaryExprAST - Expression class for a binary operator.
class BinaryExprAST : public ExprAST {
  char op;
  std::unique_ptr<ExprAST> lhs, rhs;

 public:
  BinaryExprAST(char op, std::unique_ptr<ExprAST> lhs,
                std::unique_ptr<ExprAST> rhs)
      : op(op), lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) override;
};

// CallExprAST - Expression class for function calls.
class CallExprAST : public ExprAST {
  std::string callee;
  std::vector<std::unique_ptr<ExprAST>> args;
  std::string current_func;
  std::vector<arg_type> args_type;

 public:
  CallExprAST(const std::string &callee,
              std::vector<std::unique_ptr<ExprAST>> args,
              std::string &current_func, std::vector<arg_type> args_type)
      : callee(callee), args(std::move(args)) {
    current_func = current_func;
    args_type = args_type;
  }

  peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) override;
};

/// IfExprAST - Expression class for if/then/else.
class IfExprAST : public ExprAST {
  std::unique_ptr<ExprAST> cond_expr, then_stmt, else_stmt;

 public:
  IfExprAST(std::unique_ptr<ExprAST> cond_expr,
            std::unique_ptr<ExprAST> then_stmt,
            std::unique_ptr<ExprAST> else_stmt)
      : cond_expr(std::move(cond_expr)),
        then_stmt(std::move(then_stmt)),
        else_stmt(std::move(else_stmt)) {}

  peloton::codegen::Value Codegen(
      peloton::codegen::CodeGen &codegen,
      peloton::codegen::FunctionBuilder &fb) override;
};

// FunctionAST - This class represents a function definition itself.
class FunctionAST {
  std::unique_ptr<ExprAST> body;

 public:
  FunctionAST(std::unique_ptr<ExprAST> body) : body(std::move(body)) {}

  llvm::Function *Codegen(peloton::codegen::CodeGen &codegen,
                          peloton::codegen::FunctionBuilder &fb);
};

/*----------------------------------------------------------------
/// Error* - These are little helper functions for error handling.
-----------------------------------------------------------------*/

std::unique_ptr<ExprAST> LogError(const char *str);
peloton::codegen::Value LogErrorV(const char *str);

}  // namespace udf
}  // namespace peloton
