//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_compiler.cpp
//
// Identification: src/codegen/query_compiler.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"

#include "codegen/compilation_context.h"

namespace peloton {
namespace codegen {

// Constructor
QueryCompiler::QueryCompiler() : next_id_(0) {}

// Compile the given query statement
std::unique_ptr<QueryStatement> QueryCompiler::Compile(
    const planner::AbstractPlan &root, QueryResultConsumer &result_consumer,
    CompileStats *stats) {
  // The query statement we compile
  std::unique_ptr<QueryStatement> query{new QueryStatement(root)};

  // Set up the compilation context
  RuntimeState runtime_state;
  TranslatorFactory translator_factory;
  CompilationContext context{query->GetCodeContext(), runtime_state,
                             translator_factory, result_consumer};

  // Perform the compilation
  context.GeneratePlan(*query, stats);

  // Return the compiled query statement
  // Note: we don't need to move because the "query" variable can be converted
  // to an rvalue and moved
  return query;
}

}  // namespace codegen
}  // namespace peloton