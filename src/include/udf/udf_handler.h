//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_handler.h
//
// Identification: src/include/udf/udf_handler.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "type/type_id.h"

namespace llvm {
class Function;
class Type;
}  // namespace llvm

namespace peloton {

namespace codegen {
class CodeContext;
class CodeGen;
}  // namespace codegen

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace expression {
class FunctionExpression;
}  // namespace expression

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