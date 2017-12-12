//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_compiler.h
//
// Identification: src/include/codegen/query_compiler.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <memory>

#include "codegen/query.h"
#include "codegen/query_parameters_map.h"
#include "codegen/query_result_consumer.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}  // namespace plan

namespace codegen {

// The primary interface to JIT compile queries
class QueryCompiler {
 public:
  //===--------------------------------------------------------------------===//
  // A tiny struct that collects statistics on query compilation.  In here, we
  // can track the amount of time it took to setup the compilation, the amount
  // of time it took to generate the LLVM IR and the amount of time it took to
  // JIT compile the query's components into native code.
  //===--------------------------------------------------------------------===//
  struct CompileStats {
    // The time taken to setup the compilation context
    double setup_ms = 0.0;

    // The time taken to generate all the IR for the plan
    double ir_gen_ms = 0.0;

    // The time taken to perform JIT compilation
    double jit_ms = 0.0;
  };

  // Constructor
  QueryCompiler();

  // Compile the provided query, returning the compiled plan that can be invoked
  // to return results. Callers can also pass in an (optional) CompileStats
  // object pointer if they want to collect statistics on the compilation
  // process.
  std::unique_ptr<Query> Compile(const planner::AbstractPlan &query_plan,
                                 const QueryParametersMap &parameters_map,
                                 QueryResultConsumer &consumer,
                                 CompileStats *stats = nullptr);

  // Get the next available query plan ID
  uint64_t NextId() { return next_id_++; }

  static bool IsSupported(const planner::AbstractPlan &plan);
  static bool IsExpressionSupported(const expression::AbstractExpression &plan);

 private:

  // Counter we use to ID the queries we compiled
  std::atomic<uint64_t> next_id_;
};

}  // namespace codegen
}  // namespace peloton
