//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_translator.h
//
// Identification: src/include/codegen/delete/delete_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/pipeline.h"
#include "planner/delete_plan.h"
#include "codegen/table.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a delete operator
//===----------------------------------------------------------------------===//
class DeleteTranslator : public OperatorTranslator {
 public:
  DeleteTranslator(const planner::DeletePlan &delete_plan,
                   CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  void DefineAuxiliaryFunctions() override {}

  void TearDownState() override {}

  std::string GetName() const override { return "delete"; }

  void Produce() const override;

  void Consume(ConsumerContext &, RowBatch &) const override;
  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  bool wrapper(oid_t tile_group_id, oid_t tuple_id, concurrency::Transaction *txn, storage::DataTable *table);
  const planner::DeletePlan &delete_plan_;
  codegen::Table table_;

  //===--------------------------------------------------------------------===//
  // A structure that proxies delete wrapper
  //===--------------------------------------------------------------------===//
  struct _DeleteWrapper {
    // Return the symbol for the Manager.GetTableWithOid() function
    static const std::string &GetFunctionName();
    // Return the LLVM-typed function definition for Manager.GetTableWithOid()
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};


}  // namespace codegen
}  // namespace peloton
