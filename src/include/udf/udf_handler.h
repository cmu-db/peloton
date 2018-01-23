#include "expression/function_expression.h"
#include "type/value.h"
#include "udf/udf_parser.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace udf {

using arg_type = type::TypeId;

class UDFHandler {
 public:
  UDFHandler();

  std::shared_ptr<codegen::CodeContext> Execute(
      concurrency::TransactionContext *txn, std::string func_name,
      std::string func_body, std::vector<std::string> args_name,
      std::vector<arg_type> args_type, arg_type ret_type);
  llvm::Function *RegisterExternalFunction(
      peloton::codegen::CodeGen &codegen,
      const expression::FunctionExpression &func_expr);

 private:
  std::shared_ptr<codegen::CodeContext> Compile(
      concurrency::TransactionContext *txn, std::string func_name,
      std::string func_body, std::vector<std::string> args_name,
      std::vector<arg_type> args_type, arg_type ret_type);

  llvm::Type *GetCodegenParamType(arg_type type_val,
                                  peloton::codegen::CodeGen &cg);
};

}  // namespace udf
}  // namespace peloton